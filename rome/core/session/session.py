import logging
import uuid

from rome.core.utils import current_milli_time
from rome.driver.redis.lock import ClusterLock

from rome.core.session.utils import ObjectSaver
from rome.driver.database_driver import get_driver
from oslo_db.exception import DBDeadlock


class SessionDeadlock(Exception):
    pass


class SessionControlledExecution(object):

    def __init__(self, session, max_try=1):
        self.session = session
        self.current_try_count = 0
        self.max_try = max_try

    def __enter__(self):
        pass

    def __exit__(self, type, value, traceback):
        if traceback:
            logging.info(traceback)
        else:
            self.session.flush()


def already_in(obj, objects):
    """
    Checks if a python object is already in a list of python objects
    :param obj: a python object
    :param objects: a list of python objects
    :return: True if the object is already in the given list. False in the other case.
    """
    if obj in objects:
        return True
    obj_signature = "%s" % (obj)
    existing_signature = map(lambda x: "%s" % (x), objects)
    return obj_signature in existing_signature


class Session(object):

    max_duration = 300

    def __init__(self, check_version_numbers=True):
        self.session_id = uuid.uuid1()
        self.session_objects_add = []
        self.session_objects_delete = []
        self.session_timeout = current_milli_time() + Session.max_duration
        self.check_version_numbers = check_version_numbers
        self.dlm = ClusterLock()
        self.acquired_locks = []
        self.already_saved = []

    def add(self, *objs):
        """
        Add the given objects to the list of objects that should be committed via this session.
        :param objs: a list of python objects
        """
        for obj in objs:
            if hasattr(obj, "is_loaded"):
                if obj.is_loaded:
                    obj = obj.data
                else:
                    continue
            if not already_in(obj, self.session_objects_add):
                self.session_objects_add += [obj]

    def add_all(self, objs):
        """
        Add the given objects to the list of objects that should be committed via this session.
        :param objs: a list of python objects
        """
        for obj in objs:
            self.add(obj)

    def update(self, obj):
        """
        Add the given objects to the list of objects that should be committed via this session. If
        the object was already in the list of objects that should be committed, its previous value
        will be replaced by the one given as a parameter.
        :param obj: a list of python objects
        """
        if already_in(obj, self.session_objects_add):
            filtered = filter(lambda x: ("%s" % (x)) != "%s" % (obj),
                              self.session_objects_add)
            self.session_objects_add = filtered
        if not already_in(obj, self.session_objects_add):
            self.session_objects_add += [obj]

    def delete(self, *objs):
        """
        Add the given objects to the list of objects that should be deleted via this session.
        :param objs: a list of python objects
        """
        for obj in objs:
            if hasattr(obj, "is_loaded"):
                if obj.is_loaded:
                    obj = obj.data
                else:
                    continue
            if not already_in(obj, self.session_objects_delete):
                self.session_objects_delete += [obj]

    def query(self, *args, **kwargs):
        """
        Provide a new query object
        :param args: arguments of the query
        :param kwargs: key/value arguments of the query
        :return: a Query object
        """
        from rome.core.orm.query import Query
        kwargs["__session"] = self
        query = Query(*args, **kwargs)
        return query

    def begin(self, *args, **kwargs):
        return SessionControlledExecution(session=self)

    def flush(self, *args, **kwargs):
        logging.info("flushing session %s" % (self.session_id))
        if self.can_commit_request():
            logging.info("committing session %s" % (self.session_id))
            self.commit()

    def can_be_used(self, obj):
        if getattr(obj, "_session", None) is None:
            return True
        else:
            if obj.session["session_id"] == self.session_id:
                return True
            if current_milli_time >= obj.session["session_timeout"]:
                return True
        logging.error("session %s cannot use object %s" %
                      (self.session_id, obj))
        return False

    def can_commit_request(self):
        locks = []
        success = True
        # Acquire lock on each objects of the session
        for obj in self.session_objects_add + self.session_objects_delete:
            if obj.id is not None:
                lock_name = "session_lock_%s_%s" % (obj.__tablename__, obj.id)
                if self.dlm.lock(lock_name, 100):
                    locks += [lock_name]
                else:
                    success = False
                    break
        if success and self.check_version_numbers:
            # Check the version number of each object
            driver = get_driver()
            for obj in self.session_objects_add + self.session_objects_delete:
                db_current_version = driver.get_object_version_number(obj.__table__.name, obj.id)
                version_number = getattr(obj, "___version_number", None)
                if db_current_version != version_number:
                    success = False
                    break
        # Now, we can commit or abort the modifications
        if not success:
            logging.error("session %s encountered a conflict, aborting commit" %
                          (self.session_id))
            for lock in locks:
                self.dlm.unlock(lock)
            raise DBDeadlock()
        else:
            logging.error("session %s has been committed" %
                          (self.session_id))
            self.acquired_locks = locks
        return success

    def commit(self):
        logging.info("session %s will start commit" % (self.session_id))
        object_saver = ObjectSaver(self)
        for obj in self.session_objects_add:
            object_saver.save(obj)
        for obj in self.session_objects_delete:
            object_saver.delete(obj)
        logging.info("session %s committed" % (self.session_id))
        for lock in self.acquired_locks:
            self.dlm.unlock(lock)
            self.acquired_locks.remove(lock)
        self.session_objects_add = []
        self.session_objects_delete = []