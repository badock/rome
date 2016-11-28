"""JSON module.

This module contains functions, classes and mix-in that are used for the
simplification of objects, before storing them into the JSON database.

"""

import datetime
import uuid

import netaddr
import pytz
from rome.utils.limited_size_dictionnary import LimitedSizeDictionnary

import rome.core.utils as utils
from rome.core.utils import DATE_FORMAT

CACHES = LimitedSizeDictionnary(size_limit=20)
SIMPLE_CACHES = LimitedSizeDictionnary(size_limit=20)
COMPLEX_CACHES = LimitedSizeDictionnary(size_limit=20)
TARGET_CACHES = LimitedSizeDictionnary(size_limit=20)


def extract_adress(obj):
    """Extract an indentifier for the given object: if the object contains an
    id, it returns the id, otherwise it returns the memory address of the
    given object."""

    result = hex(id(obj))
    try:
        if utils.is_novabase(obj):
            result = str(obj).split("at ")[1].split(">")[0]
    except Exception:
        pass
    return result


class Encoder(object):
    """A class that is in charge of converting python objects (basic types,
    dictionnaries, novabase objects, ...) to a representation that can
    be stored in database."""

    def __init__(self, request_uuid=uuid.uuid1()):
        self.request_uuid = (request_uuid if request_uuid is not None else
                             uuid.uuid1())
        if not SIMPLE_CACHES.has_key(self.request_uuid):
            SIMPLE_CACHES[self.request_uuid] = {}
        if not COMPLEX_CACHES.has_key(self.request_uuid):
            COMPLEX_CACHES[self.request_uuid] = {}
        if not TARGET_CACHES.has_key(self.request_uuid):
            TARGET_CACHES[self.request_uuid] = {}

        self.simple_cache = SIMPLE_CACHES[self.request_uuid]
        self.complex_cache = COMPLEX_CACHES[self.request_uuid]
        self.target_cache = TARGET_CACHES[self.request_uuid]

        self.reset()

    def get_cache_key(self, obj):
        """Compute an "unique" key for the given object: this key is used to
        when caching objects."""

        classname = obj.__class__.__name__
        if classname == "LazyReference" or classname == "LazyValue":
            return obj.get_key()

        if hasattr(obj, "id") and getattr(obj, "id") is not None:
            key = "%s_%s" % (classname, obj.id)
        else:
            key = "%s_x%s" % (classname, extract_adress(obj))
        return key

    def already_processed(self, obj):
        """Check if the given object has been processed, according to its
        unique key."""
        if hasattr(obj, "is_relationship_list"):
            return True
        key = self.get_cache_key(obj)
        return self.simple_cache.has_key(key)

    def datetime_simplify(self, datetime_ref):
        """Simplify a datetime object."""

        return {
            "simplify_strategy": "datetime",
            "value": datetime_ref.strftime(DATE_FORMAT),
            "timezone": str(datetime_ref.tzinfo)
        }

    def ipnetwork_simplify(self, ipnetwork):
        """Simplify an IP address object."""

        return {"simplify_strategy": "ipnetwork", "value": str(ipnetwork)}

    def process_field(self, field_value):
        """Inner function that processes a value."""

        if utils.is_novabase(field_value):
            if not self.already_processed(field_value):
                self.process_object(field_value, False)
            key = self.get_cache_key(field_value)
            result = self.simple_cache[key]
        else:
            result = self.process_object(field_value, False)
        return result

    def process_object(self, obj, skip_reccursive_call=True):
        """Apply the best simplification strategy to the given object."""

        if obj.__class__.__name__ == "datetime":
            result = self.datetime_simplify(obj)
        elif obj.__class__.__name__ == "IPNetwork":
            result = self.ipnetwork_simplify(obj)
        else:
            result = obj

        return result

    def reset(self):
        """Reset the caches of the current instance of Simplifier."""

        self.simple_cache = {}
        self.complex_cache = {}
        self.target_cache = {}

    def encode(self, obj):
        """Simplify the given object."""

        result = self.process_object(obj, False)
        return result


def convert_to_camelcase(word):
    """Convert the given word into camelcase naming convention."""
    return ''.join(x.capitalize() or '_' for x in word.split('_'))


def is_dict_and_has_key(obj, key):
    """Check if the given object is a dict which contains the given key."""
    if isinstance(obj, dict):
        return obj.has_key(key)
    return False


class Decoder(object):
    """Class that translate an object containing values taken from database
    into an object containing values understandable by services composing
    Nova."""

    def __init__(self, request_uuid=uuid.uuid1()):
        """Constructor"""
        self.request_uuid = (request_uuid if request_uuid is not None else
                             uuid.uuid1())
        if not CACHES.has_key(self.request_uuid):
            CACHES[self.request_uuid] = {}
        self.cache = CACHES[self.request_uuid]

    def get_key(self, obj):
        """Returns a unique key for the given object."""
        if is_dict_and_has_key(obj, "_nova_classname"):
            table_name = obj["_nova_classname"]
            key = obj["id"]
            return "%s_%s_%s" % (table_name, str(key), self.request_uuid)
        elif is_dict_and_has_key(obj, "novabase_classname"):
            table_name = obj["novabase_classname"]
            key = obj["id"]
            return "%s_%s_%s" % (table_name, str(key), self.request_uuid)
        else:
            return "%s_%s_%s" % (hex(id(obj)), hex(id(obj)), self.request_uuid)

    def datetime_desimplify(self, value):
        """Desimplify a datetime object."""
        result = datetime.datetime.strptime(value["value"], DATE_FORMAT)
        if value["timezone"] == "UTC":
            result = pytz.utc.localize(result)
        return result

    def ipnetwork_desimplify(self, value):
        """Desimplify an IPNetwork object."""
        return netaddr.IPNetwork(value["value"])

    def decode(self, obj):
        """Apply the best desimplification strategy on the given object."""
        result = obj
        is_dict = isinstance(obj, dict)
        is_list = isinstance(obj, list)
        if is_dict_and_has_key(obj, "simplify_strategy"):
            if obj['simplify_strategy'] == 'datetime':
                result = self.datetime_desimplify(obj)
            if obj['simplify_strategy'] == 'ipnetwork':
                result = self.ipnetwork_desimplify(obj)
        elif is_list:
            result = []
            for item in obj:
                result += [self.decode(item)]
        elif is_dict:
            result = {}
            for item in obj:
                result[item] = self.decode(obj[item])
        return result
