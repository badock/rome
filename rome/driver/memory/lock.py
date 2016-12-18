# This code is inspired by the code found at, in order to provide
# an implementation with the new cluster architecture provided by
# redis 3:
#   https://github.com/SPSCommerce/redlock-py

import random
import threading
import time

from rome.core.utils import current_milli_time

# Python 3 compatibility
string_type = getattr(__builtins__, 'basestring', str)


class MemoryLock(object):

    def __init__(self):
        self.locks = {}
        self.modification_lock = threading.Lock()
        self.retry_count = 1

    def lock(self, name, ttl):
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
        with self.modification_lock:
            if name in self.locks:
                self.locks.pop(name)

