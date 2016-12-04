from rome.conf.configuration import get_config


class DatabaseDriverInterface(object):

    def add_key(self, tablename, key):
        raise NotImplementedError

    def remove_key(self, tablename, key):
        raise NotImplementedError

    def next_key(self, tablename):
        raise NotImplementedError

    def keys(self, tablename):
        raise NotImplementedError

    def put(self, tablename, key, value, secondary_indexes=[]):
        raise NotImplementedError

    def get_version_number(self, tablename):
        raise NotImplementedError

    def get_object_version_number(self, tablename, key):
        raise NotImplementedError

    def get(self, tablename, key, hint=None):
        raise NotImplementedError

    def getall(self, tablename, hints=[]):
        raise NotImplementedError


driver = None


def build_driver():
    from rome.driver.redis.driver import RedisClusterDriver, RedisDriver
    from rome.driver.etcd.driver import EtcdDriver

    config = get_config()
    backend = config.backend()
    # backend = "etcd"

    if backend == "redis":
        if config.redis_cluster_enabled():
            return RedisClusterDriver()
        else:
            return RedisDriver()
    return EtcdDriver()


def get_driver():
    global driver
    if driver is None:
        driver = build_driver()
    return driver
