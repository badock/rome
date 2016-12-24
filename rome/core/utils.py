"""Utils module.

This module contains functions, classes and mix-in that are used for the
discovery database backend.

"""

import time
import rome.driver.database_driver as database_driver
from sqlalchemy.orm.collections import CollectionAdapter
from sqlalchemy.orm.state import InstanceState

DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def datetime_to_int(date_object):
    """
    Convert a datetime object to an integer value.
    :param date_object: a datetime object
    :return: an integer value
    """
    return int(date_object.strftime('%s'))


def current_milli_time():
    """
    Return the current time as an integer value.
    :return: an integer value
    """
    return int(round(time.time() * 1000))


def merge_dicts(dict1, dict2):
    """
    Merge two dictionnaries into one dictionnary: the values containeds inside dict2 will erase
    values of dict1.
    :param dict1: a python dictionary
    :param dict2: a python dictionary
    :return: a python dictionary that corresponds to the merge between the two given a python
    dictionary
    """
    return dict(dict1.items() + dict2.items())


def get_objects(tablename,
                hints=None):
    """
    Get objects in database that belongs to a targeted set of objects.
    :param tablename: name of the targeted set objects
    :param hints: a list of hints that will help the database driver to find objects by using
    secondary indexes
    :return: a list of python objects (dictionary representation)
    """
    if hints is None:
        hints = []
    return database_driver.get_driver().getall(tablename, hints=hints)


class CollectionAdapterWithoutEvents(CollectionAdapter):

    def append_with_event(self, item, initiator=None):
        pass


class InstanceStateWithoutBackref(InstanceState):

    def _modified_event(
            self, dict_, attr, previous, collection=False, force=False):
        pass


class LazyRelationship(object):

    def __init__(self, query, _class, many=True, request_uuid=None, info=None):
        self.query = query
        self.many = many
        self.data = None
        self.is_loaded = False
        self._class = _class
        self.info = info

    def load(self):
        """
        Load from database data that is corresponding to the lazy relationship
        """
        if self.is_loaded is False:
            from rome.core.session.utils import ObjectAttributeRefresher
            object_attribute_refresher = ObjectAttributeRefresher()
            if self.many:
                self.data = self.query.all()
                for obj in self.data:
                    if obj is not None:
                        object_attribute_refresher.refresh(obj)
            else:
                self.data = self.query.first()
                if self.data is not None:
                    object_attribute_refresher.refresh(self.data)
            self.is_loaded = True

    def __getattr__(self, item):
        if item in ["_sa_adapter"]:
            from sqlalchemy.orm.collections import InstrumentedList
            from sqlalchemy.orm.attributes import CollectionAttributeImpl
            from sqlalchemy.orm.state import InstanceState

            attr = CollectionAttributeImpl(self._class, "id", None, None)
            state = InstanceState(self._class(), None)
            instrumented_list = InstrumentedList()

            return CollectionAdapterWithoutEvents(attr, state, instrumented_list)
        if item in ["_sa_instance_state"]:
            fake_instance = self._class()
            class_manager = getattr(fake_instance, "_sa_class_manager")
            return InstanceStateWithoutBackref(fake_instance, class_manager)
        if item not in ["data", "many", "query", "_class", "is_loaded", "info"]:
            self.load()
        if item == "iteritems":
            if self.is_relationship_list:
                return self.data.iteritems
        if item == "__nonzero__" and self.is_relationship_list:
            return getattr(self.data, "__len__", None)
        return getattr(self.data, item, None)

    def __setattr__(self, name, value):
        if name in ["data", "many", "query", "_class", "is_loaded", "__emulates__", "info"]:
            self.__dict__[name] = value
        else:
            self.load()
            setattr(self.data, name, value)
            return self

    def __getitem__(self, item):
        self.load()
        return self.data.__getitem__(item)

    def __eq__(self, other):
        """Override the default Equals behavior"""
        if isinstance(other, self.__class__):
            for key in ["info", "many", "_class"]:
                if str(getattr(self, key)) != str(getattr(other, key)):
                    return False
        return True
