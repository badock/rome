from rome.conf.configuration import get_config


class DatabaseDriverInterface(object):

    def add_key(self, tablename, key):
        """
        Add the given key to the keys associated to a table.
        :param tablename: a table name
        :param key: a key
        """
        raise NotImplementedError

    def remove_key(self, tablename, key):
        """
        Remove a key from the keys associated to a table.
        :param tablename: a table name
        :param key: a key
        """
        raise NotImplementedError

    def next_key(self, tablename):
        """
        Increment the value of the next key and return it.
        :param tablename: a table name
        :return: a string that can be used as a key for a new object
        """
        raise NotImplementedError

    def keys(self, tablename):
        """
        Return the keys that are registered in the given table.
        :param tablename: a table name
        :return: a list of keys
        """
        raise NotImplementedError

    def put(self, tablename, key, value, secondary_indexes=None):
        """
        Insert a value in a table.
        :param tablename: a table name
        :param key: a string key
        :param value: a value
        :param secondary_indexes: (facultative) a list of secondary index
        :return: a dict that represents the value that has been inserted
        """
        raise NotImplementedError

    def get_version_number(self, tablename):
        """
        Return the version number of a table: each time a modification is made on a object of the
        table, the table version number is incremented.
        :param tablename: a table name
        :return: an integer that corresponds to the version number of the table
        """
        raise NotImplementedError

    def get_object_version_number(self, tablename, key):
        """
        Return the version number of a an object of a table: each time a modification is made on an
        object, its version number is incremented.
        :param tablename: a table name
        :param key: the key of an object
        :return: an integer that corresponds to the version number of the object
        """
        raise NotImplementedError

    def get(self, tablename, key, hint=None):
        """
        Get an object from a given table.
        :param tablename: a table name
        :param key: an object key
        :param hint: (facultative) a secondary index hint
        :return: a python dictionary
        """
        raise NotImplementedError

    def getall(self, tablename, hints=None):
        """
        Get all objects from a given table.
        :param tablename: a table name
        :param hints: (facultative) a list of secondary index hints
        :return: a python dictionary
        """
        raise NotImplementedError


DRIVER = None


def build_driver():
    """
    Build a database driver based on 'DatabaseDriverInterface'.
    :return: an Implementation of the 'DatabaseDriverInterface' class
    """
    from rome.driver.redis.driver import build_redis_driver
    from rome.driver.etcd.driver import EtcdDriver
    from rome.driver.memory.driver import MemoryDriver

    config = get_config()
    backend = config.backend()
    backend = "memory"

    if backend == "redis":
        build_redis_driver(clustered=config.redis_cluster_enabled())
    if backend == "etcd":
        return EtcdDriver()

    return MemoryDriver()


def get_driver():
    """
    Return a singleton instance of the 'DatabaseDriverInterface' class.
    :return: an instance of the 'DatabaseDriverInterface' class
    """
    global DRIVER
    if DRIVER is None:
        DRIVER = build_driver()
    return DRIVER
