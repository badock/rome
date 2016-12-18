__author__ = 'jonathan'

from collections import OrderedDict

# following code inspired from:
# http://stackoverflow.com/questions/2437617/limiting-the-size-of-a-python-dictionary


class DictionaryWithLimitedSize(OrderedDict):

    """A python dictionary that has a limited size."""

    def __init__(self, *args, **kwds):
        self.size_limit = kwds.pop("size_limit", None)
        OrderedDict.__init__(self, *args, **kwds)
        self._remove_old_items()

    def __setitem__(self, key, value):
        """
        Set a key/value pair in the dict. At the end, this function will check if old objects
        should be removed.
        :param key: a string key
        :param value: a value
        """
        OrderedDict.__setitem__(self, key, value)
        self._remove_old_items()

    def _remove_old_items(self):
        """
        Remove the items that are old.
        """
        if self.size_limit is not None:
            while len(self) > self.size_limit:
                self.popitem(last=False)
