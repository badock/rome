from rome.conf.configuration import get_config


class LockDriverInterface(object):

    def lock(self, name, ttl):
        raise NotImplementedError

    def unlock(self, name, only_expired=False):
        raise NotImplementedError


driver = None


def build_driver():
    from rome.driver.redis.lock import ClusterLock
    from rome.driver.memory.lock import MemoryLock

    config = get_config()
    backend = config.backend()
    backend = "memory"

    if backend == "redis":
        return ClusterLock()

    return MemoryLock()


def get_driver():
    global driver
    if driver is None:
        driver = build_driver()
    return driver
