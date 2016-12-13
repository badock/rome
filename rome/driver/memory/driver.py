from rome.driver.database_driver import DatabaseDriverInterface
from threading import Lock


class MemoryDriver(DatabaseDriverInterface):

    def __init__(self):
        self.database = {
            "keys": {},
            "tables": {},
            "sec_indexes": {},
            "next_keys": {},
            "version_numbers": {},
            "object_version_numbers": {}
        }

    def _init_table(self, tablename):
        if tablename not in self.database["keys"]:
            self.database["keys"][tablename] = []
        if tablename not in self.database["tables"]:
            self.database["tables"][tablename] = {}
        if tablename not in self.database["sec_indexes"]:
            self.database["sec_indexes"][tablename] = {}
        if tablename not in self.database["next_keys"]:
            self.database["sec_indexes"][tablename] = 1
        if tablename not in self.database["version_numbers"]:
            self.database["version_numbers"][tablename] = 0
        if tablename not in self.database["object_version_numbers"]:
            self.database["object_version_numbers"][tablename] = {}

    def add_key(self, tablename, key):
        if tablename not in self.database["keys"]:
            self._init_table(tablename)
        self.database["keys"][tablename] += [key]

    def remove_key(self, tablename, key):
        # keys_locks.acquire()
        if tablename not in self.database["keys"]:
            filtered_keys = filter(lambda k: k != key, self.database["keys"][tablename])
            self.database["keys"][tablename] = filtered_keys
        # keys_locks.release()

    def next_key(self, tablename):
        if tablename not in self.database["next_keys"]:
            self._init_table(tablename)
        next_key = self.database["sec_indexes"][tablename]
        self.database["sec_indexes"][tablename] += 1
        print("next_key => %s" % (next_key))
        return next_key

    def keys(self, tablename):
        if tablename not in self.database["next_keys"]:
            self._init_table(tablename)
        return self.database["keys"][tablename]

    def put(self, tablename, key, value, secondary_indexes=[]):
        # Increment version numbers
        self.incr_version_number(tablename)
        self.incr_object_version_number(tablename, key)
        # Add the version number
        object_version_number = self.get_object_version_number(tablename, key)
        value["___version_number"] = object_version_number
        # Set the value in database
        self.database["tables"][tablename][key] = value

    def get_version_number(self, tablename):
        if tablename not in self.database["version_numbers"]:
            self._init_table(tablename)
        return self.database["version_numbers"][tablename]

    def incr_version_number(self, tablename):
        if tablename not in self.database["version_numbers"]:
            self._init_table(tablename)
        self.database["version_numbers"][tablename] += 1
        return self.database["version_numbers"][tablename]

    def reset_object_version_number(self, tablename, key):
        if tablename not in self.database["keys"]:
            self._init_table(tablename)
        self.database["object_version_numbers"][tablename][key] = 0

    def incr_object_version_number(self, tablename, key):
        if tablename not in self.database["keys"]:
            self._init_table(tablename)
        if key not in self.database["object_version_numbers"][tablename]:
            self.database["object_version_numbers"][tablename][key] = 0
        self.database["object_version_numbers"][tablename][key] += 1
        return self.database["object_version_numbers"][tablename]

    def get_object_version_number(self, tablename, key):
        if tablename not in self.database["object_version_numbers"]:
            self._init_table(tablename)
        if key in self.database["object_version_numbers"][tablename]:
            return self.database["object_version_numbers"][tablename][key]
        else:
            return 0

    def get(self, tablename, key, hint=None):
        if tablename not in self.database["object_version_numbers"]:
            self._init_table(tablename)
        if key in self.database["tables"]:
            return self.database["tables"][key]
        else:
            return None

    def getall(self, tablename, hints=[]):
        if not tablename in self.database["object_version_numbers"]:
            self._init_table(tablename)
        result = map(lambda (k,v) : v, self.database["tables"][tablename].iteritems())
        return result