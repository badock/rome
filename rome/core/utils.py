"""Utils module.

This module contains functions, classes and mix-in that are used for the
discovery database backend.

"""

import time

import rome.driver.database_driver as database_driver

try:
    from oslo.utils import timeutils
except:
    from oslo_utils import timeutils

# from oslo.db.exception import DBDeadlock
try:
    from oslo.db.exception import DBDeadlock
except:
    from oslo_db.exception import DBDeadlock

DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def datetime_to_int(x):
    return int(x.strftime('%s'))


current_milli_time = lambda: int(round(time.time() * 1000))


def merge_dicts(dict1, dict2):
    """Merge two dictionnaries into one dictionnary: the values containeds
    inside dict2 will erase values of dict1."""
    return dict(dict1.items() + dict2.items())


def current_milli_time():
    return int(round(time.time() * 200))


def get_objects(tablename, desimplify=True, request_uuid=None, skip_loading=False, hints=[]):
    return database_driver.get_driver().getall(tablename, hints=hints)


class LazyRelationship(object):
    def __init__(self, query, _class, many=True, request_uuid=None):
        self.query = query
        self.many = many
        self.data = None
        self.is_loaded = False
        self._class = _class
        if self.many:
            self.data = []
        else:
            self.data = self._class()

    def reload(self):
        if self.data is None:
            if self.many:
                self.data = self.query.all()
            else:
                for (k, v) in self.query.first():
                    setattr(self.data, k, v)

    def __getattr__(self, item):
        if item not in ["data", "many", "query", "_class", "is_loaded"]:
            self.reload()
        if item == "iteritems":
            if self.is_relationship_list:
                return self.data.iteritems
            else:
                None
        if item == "__nonzero__" and self.is_relationship_list:
            return getattr(self.data, "__len__", None)
        return getattr(self.data, item, None)

    def __setattr__(self, name, value):
        if name in ["data", "many", "query", "_class", "is_loaded"]:
            self.__dict__[name] = value
        else:
            self.reload()
            setattr(self.data, name, value)
            return self
