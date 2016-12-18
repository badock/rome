import random
import threading
import time

from rome.core.utils import current_milli_time


class MemoryLock(object):

    def __init__(self):
        self.locks = {}
        self.modification_lock = threading.Lock()
        self.retry_count = 1

    def lock(self, name, ttl):
        """
        Acquire a lock.
        :param name: a lock name
        :param ttl: time to live
        :return: a boolean value that is True if the lock has been aquired and False in the other
        case.
        """
        retry = 0
        while retry < self.retry_count:
            with self.modification_lock:
                if "name" not in self.locks:
                    self.locks[name] = {
                        "name": name,
                        "ttl": ttl,
                        "time": current_milli_time()
                    }
                    return True
            time.sleep(random.uniform(0.005, 0.010))
        return False

    def unlock(self, name, only_expired=False):
        """
        Release a lock.
        :param name: a lock name
        :param only_expired: a boolean. When it is True, the lock is released only if the lock has
        expired.
        :return: a boolean which is True if the lock has been correctly released, and which is False
        in the other case
        """
        with self.modification_lock:
            if name in self.locks:
                self.locks.pop(name)
                return True
        return False
