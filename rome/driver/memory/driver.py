from rome.driver.database_driver import DatabaseDriverInterface


class MemoryDriver(DatabaseDriverInterface):

    """A Driver that enables to manipulate a python dictionary as it was a database."""

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
        """
        Init the local data structures for a given table.
        :param tablename: a table name
        """
        if tablename not in self.database["keys"]:
            self.database["keys"][tablename] = []
        if tablename not in self.database["tables"]:
            self.database["tables"][tablename] = {}
        if tablename not in self.database["sec_indexes"]:
            self.database["sec_indexes"][tablename] = {}
        if tablename not in self.database["next_keys"]:
            self.database["next_keys"][tablename] = 1
        if tablename not in self.database["version_numbers"]:
            self.database["version_numbers"][tablename] = 0
        if tablename not in self.database["object_version_numbers"]:
            self.database["object_version_numbers"][tablename] = {}

    def add_key(self, tablename, key):
        """
        Add the given key to the keys associated to a table.
        :param tablename: a table name
        :param key: a key
        """
        if tablename not in self.database["keys"]:
            self._init_table(tablename)
        self.database["keys"][tablename] += [key]

    def remove_key(self, tablename, key):
        """
        Remove a key from the keys associated to a table.
        :param tablename: a table name
        :param key: a key
        """
        if tablename in self.database["keys"]:
            filtered_keys = filter(lambda k: k != key, self.database["keys"][tablename])
            self.database["keys"][tablename] = filtered_keys
        if tablename in self.database["tables"]:
            if key in self.database["tables"][tablename]:
                self.database["tables"][tablename].pop(key)

    def next_key(self, tablename):
        """
        Increment the value of the next key and return it.
        :param tablename: a table name
        :return: a string that can be used as a key for a new object
        """
        if tablename not in self.database["next_keys"]:
            self._init_table(tablename)
        next_key = self.database["next_keys"][tablename]
        self.database["next_keys"][tablename] += 1
        return next_key

    def keys(self, tablename):
        """
        Return the keys that are registered in the given table.
        :param tablename: a table name
        :return: a list of keys
        """
        if tablename not in self.database["next_keys"]:
            self._init_table(tablename)
        return self.database["keys"][tablename]

    def put(self, tablename, key, value, secondary_indexes=None):
        """
        Insert a value in a table.
        :param tablename: a table name
        :param key: a string key
        :param value: a value
        :param secondary_indexes: (facultative) a list of secondary index
        :return: a dict that represents the value that has been inserted
        """
        if secondary_indexes is None:
            secondary_indexes = []
        # Increment version numbers
        self._incr_version_number(tablename)
        self._incr_object_version_number(tablename, key)
        # Add the version number
        object_version_number = self.get_object_version_number(tablename, key)
        value["___version_number"] = object_version_number
        # Set the value in database
        self.database["tables"][tablename][key] = value

    def get_version_number(self, tablename):
        """
        Return the version number of a table: each time a modification is made on a object of the
        table, the table version number is incremented.
        :param tablename: a table name
        :return: an integer that corresponds to the version number of the table
        """
        if tablename not in self.database["version_numbers"]:
            self._init_table(tablename)
        return self.database["version_numbers"][tablename]

    def _incr_version_number(self, tablename):
        """
        Increment and return the version number of the table.
        :param tablename: a table name
        :return: an integer
        """
        if tablename not in self.database["version_numbers"]:
            self._init_table(tablename)
        self.database["version_numbers"][tablename] += 1
        return self.database["version_numbers"][tablename]

    def _reset_object_version_number(self, tablename, key):
        """
        Reset and return the version number of an object from a specified table.
        :param tablename: a table name
        :param key: an object key
        :return: an integer
        """
        if tablename not in self.database["keys"]:
            self._init_table(tablename)
        self.database["object_version_numbers"][tablename][key] = 0

    def _incr_object_version_number(self, tablename, key):
        """
        Increment and return the version number of an object from a specified table.
        :param tablename: a table name
        :param key: an object key
        :return: an integer
        """
        if tablename not in self.database["keys"]:
            self._init_table(tablename)
        if key not in self.database["object_version_numbers"][tablename]:
            self.database["object_version_numbers"][tablename][key] = 0
        self.database["object_version_numbers"][tablename][key] += 1
        return self.database["object_version_numbers"][tablename]

    def get_object_version_number(self, tablename, key):
        """
        Return the version number of a an object of a table: each time a modification is made on an
        object, its version number is incremented.
        :param tablename: a table name
        :param key: the key of an object
        :return: an integer that corresponds to the version number of the object
        """
        if tablename not in self.database["object_version_numbers"]:
            self._init_table(tablename)
        if key in self.database["object_version_numbers"][tablename]:
            return self.database["object_version_numbers"][tablename][key]
        else:
            return 0

    def get(self, tablename, key, hint=None):
        """
        Get an object from a given table.
        :param tablename: a table name
        :param key: an object key
        :param hint: (facultative) a secondary index hint
        :return: a python dictionary
        """
        if tablename not in self.database["object_version_numbers"]:
            self._init_table(tablename)
        if key in self.database["tables"][tablename]:
            return self.database["tables"][tablename][key]
        else:
            return None

    def getall(self, tablename, hints=None):
        """
        Get all objects from a given table.
        :param tablename: a table name
        :param hints: (facultative) a list of secondary index hints
        :return: a python dictionary
        """
        if hints is None:
            hints = []
        if not tablename in self.database["object_version_numbers"]:
            self._init_table(tablename)
        result = map(lambda (k, v): v, self.database["tables"][tablename].iteritems())
        return result
