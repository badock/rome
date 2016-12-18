from ujson import loads as ujson_loads
from ujson import dumps as ujson_dumps

import etcd
from rome.driver.database_driver import DatabaseDriverInterface
from redlock import Redlock as Redlock

from rome.conf.configuration import get_config


def chunks(elements, chunk_size):
    """
    Generate a list of lists that corresponds to the division of a given list in several sub lists
    that each have a specified size
    :param elements: list of elements
    :param chunk_size: size of each new list
    :return: a generator that generates a list of list
    """
    for i in xrange(0, len(elements), chunk_size):
        yield elements[i:i + chunk_size]


def flatten(container):
    """
    Flatten a given list of list
    :param container: a list of list
    :return: a generator that generates a list of elements
    """
    for elements in container:
        if isinstance(elements, list) or isinstance(elements, tuple):
            for j in flatten(elements):
                yield j
        else:
            yield elements


def convert_unicode_dict_to_utf8(dictionary):
    """
    Convert a dict that has keys and values coded in unicode, to a dictionary that has keys and
    values coded in UTF8
    :param dictionary: a python dictionary
    :return: a python dictionary
    """
    if dictionary is None:
        return None
    result = {}
    for key in dictionary:
        value = dictionary[key]
        if type(value) is dict:
            value = convert_unicode_dict_to_utf8(dictionary[key])
        elif type(value) is unicode:
            value = value.encode('utf-8')
        result[str(key)] = value
    return result


class EtcdDriver(DatabaseDriverInterface):

    """A Driver that enables to manipulate an ETCD database."""

    def __init__(self):
        config = get_config()
        self.etcd_client = etcd.Client(host=config.host(), port=2379)
        self.dlm = Redlock([{"host": config.host(),
                             "port": 6379,
                             "db": 0},],
                           retry_count=10)

    def add_key(self, tablename, key):
        """
        Add the given key to the keys associated to a table.
        :param tablename: a table name
        :param key: a key
        """
        etcd_key = "%s_keys/%s" % (tablename, key)
        self.etcd_client.write(etcd_key, key)

    def remove_key(self, tablename, key):
        """
        Remove a key from the keys associated to a table.
        :param tablename: a table name
        :param key: a key
        """
        etcd_table_keys_key = "%s_keys/%s" % (tablename, key)
        etcd_key = "/%s/%s" % (tablename, key)
        self.etcd_client.delete(etcd_table_keys_key)
        self.etcd_client.delete(etcd_key)

    def next_key(self, tablename):
        """
        Increment the value of the next key and return it.
        :param tablename: a table name
        :return: a string that can be used as a key for a new object
        """
        next_key_table_key = 'next_key_%s' % (tablename)
        next_key_table_lock = 'next_key_%s_lock' % (tablename)
        lock = etcd.Lock(self.etcd_client, next_key_table_lock)
        # Use the lock object:
        lock.acquire(blocking=True, lock_ttl=20)
        lock.acquire(lock_ttl=60)
        current_key = self.etcd_client.read(next_key_table_key)
        current_key_as_int = int(current_key)
        new_key_as_int = current_key_as_int + 1
        self.etcd_client.write(next_key_table_key, new_key_as_int, prevValue=current_key)
        lock.release()
        return new_key_as_int

    def _get_value_from_results(self, result):
        """
        Extract the value field from an instance of 'EtcdResult'.
        :param result: an instance of 'EtcdResult'
        :return: the value field of the 'EtcdResult' instance
        """
        return result.value

    def keys(self, tablename):
        """
        Return the keys that are registered in the given table.
        :param tablename: a table name
        :return: a list of keys
        """
        etcd_table_keys_key = "%s_keys" % (tablename)
        try:
            fetched = self.etcd_client.read(etcd_table_keys_key)
            value = self._get_value_from_results(fetched)
            keys = value if value is not None else []
            return sorted(keys)
        except etcd.EtcdKeyNotFound:
            return []

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
        json_value = ujson_dumps(value)
        etcd_key = "%s/%s" % (tablename, key)
        etcd_sec_idx_key = "%s_%s" % (tablename, key)
        fetched = self.etcd_client.write("/%s" % (etcd_key), json_value)
        for secondary_index in secondary_indexes:
            secondary_value = value[secondary_index]
            fetched = self.etcd_client.write("sec_index/%s/%s/%s/%s" % (
                tablename, secondary_index, secondary_value, etcd_sec_idx_key
            ), etcd_sec_idx_key)
        result = value if fetched else None
        result = convert_unicode_dict_to_utf8(result)
        return result

    def get(self, tablename, key, hint=None):
        """
        Get an object from a given table.
        :param tablename: a table name
        :param key: an object key
        :param hint: (facultative) a secondary index hint
        :return: a python dictionary
        """
        etcd_key = "/%s/%s" % (tablename, key)
        if hint is not None:
            redis_keys = self.etcd_client.read("sec_index/%s/%s/%s" %
                                               (tablename, hint[0], hint[1]),
                                               recursive=True)
            etcd_key = redis_keys[0]
        try:
            fetched = self.etcd_client.read(etcd_key)
            # Parse result from JSON to python dict.
            value = self._get_value_from_results(fetched)
            result = ujson_loads(value) if value is not None else None
            return result
        except etcd.EtcdKeyNotFound:
            return None

    def _resolve_keys(self, tablename, keys):
        """
        Returns the objects that match a list of keys in a specified table.
        :param tablename: a table name
        :param keys: a list of keys
        :return: a list of python objects
        """
        fetched = list(self.etcd_client.get(tablename + "/").children)
        if len(fetched) == 0:
            return []
        str_result = map(lambda x: x.value, fetched)
        # When looking-up for a deleted object, driver return None, which should be filtered.
        str_result = filter(lambda x: x is not None, str_result)
        # Transform the list of JSON string into a single string (boost performances).
        str_result = "[%s]" % (",".join(str_result))
        # Parse result from JSON to python dict.
        result = ujson_loads(str_result)
        result = map(lambda x: convert_unicode_dict_to_utf8(x), result)
        result = filter(lambda x: x != None, result)

        return result

    def getall(self, tablename, hints=None):
        """
        Get all objects from a given table.
        :param tablename: a table name
        :param hints: (facultative) a list of secondary index hints
        :return: a python dictionary
        """
        if hints is None:
            hints = []
        if len(hints) == 0:
            keys = None
        else:
            id_hints = filter(lambda x: x[0] == "id", hints)
            non_id_hints = filter(lambda x: x[0] != "id", hints)
            sec_keys = map(lambda h: "sec_index:%s:%s:%s" %
                           (tablename, h[0], h[1]), non_id_hints)
            keys = map(lambda x: "%s:id:%s" % (tablename, x[1]), id_hints)
            for sec_key in sec_keys:
                keys += self.etcd_client.read(sec_key)
            keys = list(set(keys))
        return self._resolve_keys(tablename, keys)
