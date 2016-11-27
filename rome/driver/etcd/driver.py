import ujson

import etcd
from rome.driver.database_driver import DatabaseDriverInterface
from redlock import Redlock as Redlock

from rome.conf.Configuration import get_config

PARALLEL_STRUCTURES = {}


def easy_parallelize_multiprocessing(f, sequence):
    if not "eval_pool" in PARALLEL_STRUCTURES:
        from multiprocessing import Pool
        import multiprocessing
        NCORES = multiprocessing.cpu_count()
        eval_pool = Pool(processes=NCORES)
        PARALLEL_STRUCTURES["eval_pool"] = eval_pool
    eval_pool = PARALLEL_STRUCTURES["eval_pool"]
    result = eval_pool.map(f, sequence)
    cleaned = [x for x in result if not x is None]
    return cleaned


def easy_parallelize_sequence(f, sequence):
    if sequence is None:
        return []
    return map(f, sequence)


def easy_parallelize_gevent(f, sequence):
    if not "gevent_pool" in PARALLEL_STRUCTURES:
        from gevent.threadpool import ThreadPool
        pool = ThreadPool(30000)
        PARALLEL_STRUCTURES["gevent_pool"] = pool
    pool = PARALLEL_STRUCTURES["gevent_pool"]
    result = pool.map(f, sequence)
    return result


def easy_parallelize_eventlet(f, sequence):
    import eventlet
    green_pool_size = len(sequence) + 1
    pool = eventlet.GreenPool(size=green_pool_size)
    result = []
    for e in sequence:
        pool.spawn_n(f, e)
    pool.waitall()
    return result


easy_parallelize = easy_parallelize_sequence


def chunks(l, n):
    for i in xrange(0, len(l), n):
        yield l[i:i+n]


def flatten(container):
    for i in container:
        if isinstance(i, list) or isinstance(i, tuple):
            for j in flatten(i):
                yield j
        else:
            yield i

def convert_unicode_dict_to_utf8(d):
    if d is None:
        return None
    result = {}
    for key in d:
        v = d[key]
        if type(v) is dict:
            v = convert_unicode_dict_to_utf8(d[key])
        if type(v) is unicode:
            v = v.encode('utf-8')
        result[str(key)] = v
    return result


class EtcdDriver(DatabaseDriverInterface):

    def __init__(self):
        global eval_pool
        config = get_config()
        self.etcd_client = etcd.Client(port=2379)
        self.dlm = Redlock([{"host": "localhost", "port": 6379, "db": 0}, ], retry_count=10)

    def add_key(self, tablename, key):
        """"""
        etcd_key = "%s_keys/%s" % (tablename, key)
        self.etcd_client.write(etcd_key, key)
        pass

    def remove_key(self, tablename, key):
        """"""
        etcd_table_keys_key = "%s_keys/%s" % (tablename, key)
        etcd_key = "/%s/%s" % (tablename, key)
        self.etcd_client.delete(etcd_table_keys_key)
        self.etcd_client.delete(etcd_key)
        pass

    def next_key(self, tablename):
        """"""
        next_key_table_key = 'next_key_%s' % (tablename)
        next_key_table_lock = 'next_key_%s_lock' % (tablename)
        lock = etcd.Lock(self.etcd_client, next_key_table_lock)

        # Use the lock object:
        lock.acquire(blocking=True, # will block until the lock is acquired
              lock_ttl=None) # lock will live until we release it
        lock.is_acquired()  #
        lock.acquire(lock_ttl=60) # renew a lock
        curent_key = self.etcd_client.read(next_key_table_key)
        current_key_as_int = int(curent_key)
        new_key_as_int = current_key_as_int + 1
        self.etcd_client.write(next_key_table_key, new_key_as_int, prevValue=curent_key)
        lock.release()
        return new_key_as_int

    def keys(self, tablename):
        """"""
        """Check if the current table contains keys."""
        etcd_table_keys_key = "%s_keys" % (tablename)
        try:
            fetched = self.etcd_client.read(etcd_table_keys_key)
            keys = fetched.value if fetched.value is not None else []
            return sorted(keys)
        except etcd.EtcdKeyNotFound:
            return []

    def put(self, tablename, key, value, secondary_indexes=[]):
        """"""

        """ Dump python object to JSON field. """
        json_value = ujson.dumps(value)
        etcd_key = "%s/%s" % (tablename, key)
        etcd_sec_idx_key = "%s_%s" % (tablename, key)
        fetched = self.etcd_client.write("/%s" % (etcd_key), json_value)
        for secondary_index in secondary_indexes:
            secondary_value = value[secondary_index]
            fetched = self.etcd_client.write("sec_index/%s/%s/%s/%s" % (tablename, secondary_index, secondary_value, etcd_sec_idx_key), etcd_sec_idx_key)
        result = value if fetched else None
        result = convert_unicode_dict_to_utf8(result)
        return result

    def get(self, tablename, key, hint=None):
        """"""
        etcd_key = "/%s/%s" % (tablename, key)
        if hint is not None:
            redis_keys = self.etcd_client.read("sec_index/%s/%s/%s" % (tablename, hint[0], hint[1]), recursive=True)
            etcd_key = redis_keys[0]
        try:
            fetched = self.etcd_client.read(etcd_key)
            """ Parse result from JSON to python dict. """
            result = ujson.loads(fetched.value) if fetched is not None else None
            return result
        except etcd.EtcdKeyNotFound:
            return None

    def _resolve_keys(self, tablename, keys):

        fetched = list(self.etcd_client.get(tablename+"/").children)

        if len(fetched) == 0:
            return []

        str_result = map(lambda x: x.value, fetched)

        """ When looking-up for a deleted object, driver return None, which should be filtered."""
        str_result = filter(lambda x: x is not None, str_result)

        """ Transform the list of JSON string into a single string (boost performances). """
        str_result = "[%s]" % (",".join(str_result))

        """ Parse result from JSON to python dict. """
        result = ujson.loads(str_result)
        result = map(lambda x: convert_unicode_dict_to_utf8(x), result)
        result = filter(lambda x: x!= None, result)

        return result

    def getall(self, tablename, hints=[]):
        """"""
        if len(hints) == 0:
            keys = None
        else:
            id_hints = filter(lambda x:x[0] == "id", hints)
            non_id_hints = filter(lambda x:x[0] != "id", hints)
            sec_keys = map(lambda h: "sec_index:%s:%s:%s" % (tablename, h[0], h[1]), non_id_hints)
            keys = map(lambda x: "%s:id:%s" % (tablename, x[1]), id_hints)
            for sec_key in sec_keys:
                keys += self.etcd_client.read(sec_key)
            keys = list(set(keys))
        return self._resolve_keys(tablename, keys)
