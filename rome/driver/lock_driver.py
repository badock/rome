from rome.conf.configuration import get_config


class LockDriverInterface(object):

    def lock(self, name, ttl):
        """
        Acquire a lock.
        :param name: a lock name
        :param ttl: time to live
        :return: a boolean value that is True if the lock has been aquired and False in the other
        case.
        """
        raise NotImplementedError

    def unlock(self, name, only_expired=False):
        """
        Release a lock.
        :param name: a lock name
        :param only_expired: a boolean. When it is True, the lock is released only if the lock has
        expired.
        :return: a boolean which is True if the lock has been correctly released, and which is False
        in the other case
        """
        raise NotImplementedError


DRIVER = None


def build_driver():
    """
    Build a database driver based on 'LockDriverInterface'.
    :return: an Implementation of the 'LockDriverInterface' class
    """
    from rome.driver.redis.lock import ClusterLock
    from rome.driver.memory.lock import MemoryLock

    config = get_config()
    backend = config.backend()
    backend = "memory"

    if backend == "redis":
        return ClusterLock()

    return MemoryLock()


def get_driver():
    """
    Return a singleton instance of the 'LockDriverInterface' class.
    :return: an instance of the 'LockDriverInterface' class
    """
    global DRIVER
    if DRIVER is None:
        DRIVER = build_driver()
    return DRIVER
