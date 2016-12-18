from ujson import loads as ujson_loads
from ujson import dumps as ujson_dumps

from rome.conf.configuration import get_config
from rome.driver.database_driver import DatabaseDriverInterface

DATABASE_CACHE = {}


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


class RedisDriver(DatabaseDriverInterface):

    """A Driver that enables to manipulate a Redis database."""

    def __init__(self, redis_client):
        self.redis_client = redis_client

    def add_key(self, tablename, key):
        """
        Add the given key to the keys associated to a table.
        :param tablename: a table name
        :param key: a key
        """
        pass

    def remove_key(self, tablename, key):
        """
        Remove a key from the keys associated to a table.
        :param tablename: a table name
        :param key: a key
        """
        redis_key = "%s:id:%s" % (tablename, key)
        self.redis_client.hdel(tablename, redis_key)
        self._incr_version_number(tablename)
        self._reset_object_version_number(tablename, key)

    def next_key(self, tablename):
        """
        Increment the value of the next key and return it.
        :param tablename: a table name
        :return: a string that can be used as a key for a new object
        """
        next_key = self.redis_client.incr("nextkey:%s" % (tablename), 1)
        return next_key

    def keys(self, tablename):
        """
        Return the keys that are registered in the given table.
        :param tablename: a table name
        :return: a list of keys
        """
        keys = self.redis_client.hkeys(tablename)
        return sorted(keys)

    def _incr_version_number(self, tablename):
        """
        Increment and return the version number of the table.
        :param tablename: a table name
        :return: an integer
        """
        version_number = self.redis_client.incr("version_number:%s" % (tablename), 1)
        return version_number

    def _incr_object_version_number(self, tablename, key):
        """
        Increment and return the version number of an object from a specified table.
        :param tablename: a table name
        :param key: an object key
        :return: an integer
        """
        version_number = self.redis_client.incr("object_version_number:%s:%s" % (tablename, key),
                                                1)
        return version_number

    def _reset_object_version_number(self, tablename, key):
        """
        Reset and return the version number of an object from a specified table.
        :param tablename: a table name
        :param key: an object key
        :return: an integer
        """
        version_number = self.redis_client.set("object_version_number:%s:%s" % (tablename, key),
                                               0)
        return version_number

    def get_version_number(self, tablename):
        """
        Return the version number of a table: each time a modification is made on a object of the
        table, the table version number is incremented.
        :param tablename: a table name
        :return: an integer that corresponds to the version number of the table
        """
        version_number = self.redis_client.get("version_number:%s" % (tablename))
        return version_number

    def get_object_version_number(self, tablename, key):
        """
        Return the version number of a an object of a table: each time a modification is made on an
        object, its version number is incremented.
        :param tablename: a table name
        :param key: the key of an object
        :return: an integer that corresponds to the version number of the object
        """
        version_number = self.redis_client.get("object_version_number:%s:%s" % (tablename, key))
        if version_number is not None:
            return int(version_number)
        else:
            return 0

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

        # Increase version numbers of the table and the object
        self._incr_version_number(tablename)
        self._incr_object_version_number(tablename, key)
        # Add the version number
        object_version_number = self.get_object_version_number(tablename, key)
        value["___version_number"] = object_version_number
        # Dump python object to JSON field.
        json_value = ujson_dumps(value)
        fetched = self.redis_client.hset(tablename,
                                         "%s:id:%s" % (tablename, key),
                                         json_value)
        for secondary_index in secondary_indexes:
            secondary_value = value[secondary_index]
            sec_index_key = "sec_index:%s:%s:%s" % (tablename, secondary_index, secondary_value)
            sec_index_value = "%s:id:%s" % (tablename, key)
            fetched = self.redis_client.sadd(sec_index_key, sec_index_value)
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
        redis_key = "%s:id:%s" % (tablename, key)
        if hint is not None:
            hint_key = "sec_index:%s:%s:%s" % (tablename, hint[0], hint[1])
            redis_keys = self.redis_client.smembers(hint_key)
            redis_key = redis_keys[0]
        fetched = self.redis_client.hget(tablename, redis_key)
        # Parse result from JSON to python dict.
        result = ujson_loads(fetched) if fetched is not None else None
        return result

    def _resolve_keys(self, tablename, keys):
        """
        Returns the objects that match a list of keys in a specified table.
        :param tablename: a table name
        :param keys: a list of keys
        :return: a list of python objects
        """
        result = []
        if len(keys) > 0:
            keys = filter(lambda x: x != "None" and x != None, keys)
            sorted_keys = sorted(keys, key=lambda x: x.split(":")[-1])
            str_result = self.redis_client.hmget(tablename, sorted_keys)
            # When looking-up for a deleted object, redis's driver return None, which should be
            # filtered.
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
        # Test if a copy of the data is in the database cache
        if get_config().database_caching():
            version_number = self.get_version_number(tablename)
            hashcode = hash(str(hints))
            hash_key = "%s_%s_%s" % (tablename, version_number, hashcode)
            if hash_key in DATABASE_CACHE:
                return DATABASE_CACHE[hash_key]
        if len(hints) == 0:
            keys = self.keys(tablename)
        else:
            id_hints = filter(lambda x: x[0] == "id", hints)
            non_id_hints = filter(lambda x: x[0] != "id", hints)
            sec_keys = map(lambda h: "sec_index:%s:%s:%s" % (tablename, h[0], h[1]), non_id_hints)
            keys = map(lambda x: "%s:id:%s" % (tablename, x[1]), id_hints)
            for sec_key in sec_keys:
                keys += self.redis_client.smembers(sec_key)
        keys = list(set(keys))
        values = self._resolve_keys(tablename, keys)
        # Put the resulting data is in the database cache
        if get_config().database_caching():
            DATABASE_CACHE[hash_key] = values
        return values


def build_redis_driver(clustered=False):
    """
    Build a database driver based on 'DatabaseDriverInterface'.
    :param clustered: a boolean that is True if the redis database is clustered.
    :return: a instance of the 'RedisDriver' class.
    """
    config = get_config()
    if not clustered:
        from redis.client import StrictRedis
        client = StrictRedis(host=config.host(), port=config.port(), db=0)
    else:
        from rediscluster.client import StrictRedisCluster
        startup_nodes = map(lambda x: {
            "host": x,
            "port": "%s" % (config.port())
        }, config.cluster_nodes())
        client = StrictRedisCluster(
            startup_nodes=startup_nodes,
            decode_responses=True)
    return RedisDriver(client)
