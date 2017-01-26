import logging
import uuid

from rome.core.utils import current_milli_time
from rome.driver.lock_driver import get_driver as get_lock_driver

from rome.core.session.utils import ObjectSaver
from rome.driver.database_driver import get_driver
from oslo_db.exception import DBDeadlock
from utils import find_an_identifier


class SessionDeadlock(Exception):
    pass


class SessionControlledExecution(object):

    def __init__(self, session, max_try=1):
        self.session = session
        self.current_try_count = 0
        self.max_try = max_try

    def __enter__(self):
        pass

    def __exit__(self, _type, value, traceback):
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
        self.session_objects_watch = []
        self.session_objects_add = []
        self.session_objects_delete = []
        self.session_timeout = current_milli_time() + Session.max_duration
        self.check_version_numbers = check_version_numbers
        self.lock_manager = get_lock_driver()
        self.acquired_locks = []
        self.already_saved = []

    def watch(self, obj):
        object_hash = hash(str(obj.__dict__))
        existing_hashes = map(lambda x: x["hash"], self.session_objects_watch)
        if object_hash not in existing_hashes:
            self.session_objects_watch += [{"object": obj, "hash": object_hash}]

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
        """
        Start a transaction.
        :param args: list of arguments
        :param kwargs: key/value arguments
        :return: a SessionControlledExecution that will be used to control the execution of the
        transaction
        """
        return SessionControlledExecution(session=self)

    def flush(self, *args, **kwargs):
        """
        Commit modifications in a transactional way.
        :param args: list of arguments
        :param kwargs: key/value arguments
        """
        logging.info("processing watched objects %s" % (self.session_id))
        for watch in self.session_objects_watch:
            if hash(str(watch["object"].__dict__)) != watch["hash"]:
                self.add(watch["object"])

        logging.info("flushing session %s" % (self.session_id))
        objects_count = len(self.session_objects_add) + len(self.session_objects_delete)
        if objects_count > 0 and self.can_commit_request():
            logging.info("committing session %s" % (self.session_id))
            self.commit()

    def can_be_used(self, obj):
        """
        Check if the given object can be used by the session. This function checks if the object
        belong to another session.
        :param obj: a python object
        :return: a boolean which is True if the object can be used.
        """
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
        """
        Check if the current session can commit its modifications in a transactional way.
        :return: a boolean which is True if the transaction can be committed.
        """
        locks = []
        success = True
        # Acquire lock on each objects of the session
        for obj in self.session_objects_add + self.session_objects_delete:
            identifier = find_an_identifier(obj)
            if identifier is not None:
                lock_name = "session_lock_%s_%s" % (obj.__tablename__, identifier)
                if self.lock_manager.lock(lock_name, 100):
                    locks += [lock_name]
                else:
                    success = False
                    break
        if success and self.check_version_numbers:
            # Check the version number of each object
            driver = get_driver()
            for obj in self.session_objects_add + self.session_objects_delete:
                identifier = find_an_identifier(obj)
                db_current_version = driver.get_object_version_number(obj.__table__.name, identifier)
                version_number = getattr(obj, "___version_number", None)
                if db_current_version != -1 and version_number != None and db_current_version != version_number:
                    success = False
                    break
        # Now, we can commit or abort the modifications
        if not success:
            logging.error("sessions %s encountered a conflict, aborting commit (%s)" %
                          (self.session_id, map(lambda x: find_an_identifier(x), self.session_objects_add)))
            for lock in locks:
                self.lock_manager.unlock(lock)
            raise DBDeadlock()
        else:
            logging.info("session %s has been committed (%s)" %
                          (self.session_id, map(lambda x: find_an_identifier(x), self.session_objects_add)))
            self.acquired_locks = locks
        return success

    def commit(self):
        """
        Commit the modifications of the session.
        """
        logging.info("session %s will start commit" % (self.session_id))
        object_saver = ObjectSaver(self)
        for obj in self.session_objects_add:
            object_saver.save(obj)
        for obj in self.session_objects_delete:
            object_saver.delete(obj)
        logging.info("session %s committed (%s)" % (self.session_id, map(lambda x: find_an_identifier(x), self.session_objects_add)))
        for lock in self.acquired_locks:
            self.lock_manager.unlock(lock)
            self.acquired_locks.remove(lock)
        self.session_objects_add = []
        self.session_objects_delete = []

        # Update watch values
        for watch in self.session_objects_watch:
            watch["hash"] = hash(str(watch["object"].__dict__))

    def execute(self, clause, params=None, mapper=None, bind=None, **kw):
        from sqlalchemy.sql.dml import Insert
        from rome.driver.database_driver import get_driver
        if type(clause) is Insert:
            database_driver = get_driver()
            for value in params:
                new_key = database_driver.next_key(clause.table.name)
                value["id"] = new_key
                database_driver.put(clause.table.name, new_key, value)

    def expire(self, instance, attribute_names=None):
        pass

    def _autoflush(self):
            self.flush()
