"""JSON module.

This module contains functions, classes and mix-in that are used for the
simplification of objects, before storing them into the JSON database.

"""

import datetime
import uuid

import netaddr
import pytz
from rome.utils.dictionary_with_limited_size import DictionaryWithLimitedSize

from rome.core.utils import DATE_FORMAT

CACHES = DictionaryWithLimitedSize(size_limit=20)
SIMPLE_CACHES = DictionaryWithLimitedSize(size_limit=20)
COMPLEX_CACHES = DictionaryWithLimitedSize(size_limit=20)
TARGET_CACHES = DictionaryWithLimitedSize(size_limit=20)


def convert_to_camelcase(word):
    """
    Convert the given word into camelcase naming convention.
    :param word: a string
    :return: the string converted to the camelcase format
    """
    return ''.join(x.capitalize() or '_' for x in word.split('_'))


def is_dict_and_has_key(obj, key):
    """
    Check if the given object is a dict which contains the given key.
    :param obj:
    :param key:
    :return:
    """
    if isinstance(obj, dict):
        return obj.has_key(key)
    return False


class Encoder(object):
    """A class that is in charge of converting python objects (basic types,
    dictionaries, novabase objects, ...) to a representation that can
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

    def datetime_encode(self, datetime_ref):
        """
        Encode a datetime object.
        :param datetime_ref: a datetime object
        :return: a dict that represents a datetime
        """

        return {
            "simplify_strategy": "datetime",
            "value": datetime_ref.strftime(DATE_FORMAT),
            "timezone": str(datetime_ref.tzinfo)
        }

    def ipnetwork_encode(self, ipnetwork):
        """
        Encode an IP address object.
        :param ipnetwork: an IP address object (IPNetwork)
        :return: a dict that represents an ip address
        """

        return {
            "simplify_strategy": "ipnetwork",
            "value": str(ipnetwork)
        }

    def process_object(self, obj):
        """
        Apply the best encoding strategy to the given object.
        :param obj: a python object
        :return: a dict containing the encoded value. In the case no encoding strategy could be
        found, the original object is returned.
        """

        if obj.__class__.__name__ == "datetime":
            result = self.datetime_encode(obj)
        elif obj.__class__.__name__ == "IPNetwork":
            result = self.ipnetwork_encode(obj)
        else:
            result = obj

        return result

    def reset(self):
        """Reset the caches of the current instance of Simplifier."""

        self.simple_cache = {}
        self.complex_cache = {}
        self.target_cache = {}

    def encode(self, obj):
        """
        Encode the given object.
        :param obj: a python object
        :return: a dict containing the encoded value. In the case no encoding strategy could be
        found, the original object is returned.
        """

        result = self.process_object(obj)
        return result


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

    def datetime_decode(self, value):
        """
        Decode a datetime object encoded as a dict.
        :param value: a dict that represents a datetime
        :return: a datetime object
        """
        result = datetime.datetime.strptime(value["value"], DATE_FORMAT)
        if value["timezone"] == "UTC":
            result = pytz.utc.localize(result)
        return result

    def ipnetwork_decode(self, value):
        """
        Decode an IPNetwork object.
        :param value: a dict that represents an IPNetwork object
        :return: an IPNetwork object
        """
        return netaddr.IPNetwork(value["value"])

    def decode(self, obj):
        """
        Encode the given object.
        :param obj: a dict that represents an encoded object
        :return: the decoded object
        """
        result = obj
        is_dict = isinstance(obj, dict)
        is_list = isinstance(obj, list)
        if is_dict_and_has_key(obj, "simplify_strategy"):
            if obj['simplify_strategy'] == 'datetime':
                result = self.datetime_decode(obj)
            if obj['simplify_strategy'] == 'ipnetwork':
                result = self.ipnetwork_decode(obj)
        elif is_list:
            result = []
            for item in obj:
                result += [self.decode(item)]
        elif is_dict:
            result = {}
            for item in obj:
                result[item] = self.decode(obj[item])
        return result
