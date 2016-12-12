import logging

import rome.driver.database_driver as database_driver
from rome.core.dataformat.json import Encoder
from rome.core.utils import LazyRelationship
from sqlalchemy.orm.interfaces import ONETOMANY, MANYTOONE, MANYTOMANY
from sqlalchemy.orm.properties import ColumnProperty, RelationshipProperty
from oslo_db.exception import DBDeadlock

from rome.core.orm.query import Query
import functools
import time
import random


def retry_on_db_deadlock(func):
    """
    Naive decorator that enables to retry execution of a session if Deadlock was received.
    :param func: a python function that will be executed
    :return: an anonymous function that will wrap the execution of the function
    """
    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        while True:
            try:
                return func(*args, **kwargs)
            except DBDeadlock:
                logging.warn("Deadlock detected when running '%s': Retrying..." % (func.__name__))
                # Retry!
                time.sleep(random.uniform(0.01, 0.20))
                continue
    functools.update_wrapper(wrapped, func)
    return wrapped


def get_class_manager(obj):
    """
    Extract the class manager of an object
    :param obj: a python object
    :return: the class manager
    """
    return getattr(obj, "_sa_class_manager", None)


def recursive_getattr(obj, key, default=None):
    """
    Recursive getter. Resolve properties such as "foo.bar.x.value" on a python object.
    :param obj: a python object
    :param key: a string
    :param default: default value
    :return: the value corresponding to the composed key
    """
    sub_keys = key.split(".")
    current_key = sub_keys[0]
    if hasattr(obj, current_key):
        current_object = getattr(obj, current_key)
        if len(current_key) == 1:
            return current_object
        else:
            return recursive_getattr(current_object, ".".join(sub_keys[1:]),
                                     default)
    else:
        if default:
            return default
        else:
            raise Exception("Could not find property '%s' in %s" %
                            (current_key, obj))


class ObjectAttributeRefresher(object):

    def refresh_one_to_many(self, obj, attr_name, attr):
        """
        Refresh a one-to-many relationship of a python object.
        :param obj: a python object
        :param attr_name: name of the relationship field
        :param attr: relationship object
        :return: a boolean which is True if the refresh worked
        """
        for left, right in attr.property.local_remote_pairs:
            left_value = getattr(obj, attr_name, None)
            if left_value:
                for element in left_value:
                    element_value = getattr(element, right.name, None)
                    if element_value is None or element_value != left_value:
                        setattr(element, right.name, element_value)
        return True

    def _generate_query(self, attr, additional_expression):
        if len(attr.prop._reverse_property) > 0:
            reverse_property = list(attr.prop._reverse_property)[0]
            entity_class = reverse_property.parent.entity
            query = Query(entity_class).filter(
                attr.expression).filter(additional_expression)
            return (entity_class, query)
        else:
            logging.info(
                "Could not generate a query with those parameters: %s, %s" %
                (attr, additional_expression))
            raise Exception(
                "Could not generate a query with those parameters: %s, %s" %
                (attr, additional_expression))

    def refresh_many_to_one(self, obj, attr_name, attr):
        """
        Refresh a many-to-one relationship of a python object.
        :param obj: a python object
        :param attr_name: name of the relationship field
        :param attr: relationship object
        :return: a boolean which is True if the refresh worked
        """
        for left, right in attr.property.local_remote_pairs:
            left_value = getattr(obj, left.name, None)
            right_value = getattr(obj, attr_name, None)
            if left_value is not None:
                if right_value is None:
                    (entity_class,
                     query) = self._generate_query(attr, right.__eq__(left_value))
                    relationship_field = LazyRelationship(query, entity_class,
                                                          many=False)
                    setattr(obj, attr_name, relationship_field)
            elif right_value is not None:
                right_value_field_value = getattr(right_value, right.name, None)
                if right_value_field_value:
                    new_left_value = getattr(obj, left.name, right_value_field_value)
                    setattr(obj, left.name, new_left_value)
        return True

    def refresh_many_to_many(self, obj, attr_name, attr):
        """
        Refresh a many-to-many relationship of a python object.
        :param obj: a python object
        :param attr_name: name of the relationship field
        :param attr: relationship object
        :return: a boolean which is True if the refresh worked
        """
        for left, right in attr.property.local_remote_pairs:
            left_value = getattr(obj, attr_name, None)
            if left_value:
                for element in left_value:
                    element_value = getattr(element, right.name, None)
                    if element_value is None or element_value != left_value:
                        setattr(element, right.name, element_value)
        return True

    def refresh(self, obj):
        result = {}
        for attr_name, attr in get_class_manager(obj).local_attrs.iteritems():
            if type(attr.property) is RelationshipProperty:
                if attr.property.direction is ONETOMANY:
                    self.refresh_one_to_many(obj, attr_name, attr)
                elif attr.property.direction is MANYTOONE:
                    self.refresh_many_to_one(obj, attr_name, attr)
                elif attr.property.direction is MANYTOMANY:
                    self.refresh_many_to_many(obj, attr_name, attr)
                else:
                    logging.error(
                        "Could not understand how to refresh the property '%s' of %s"
                        % (attr, obj))
                    raise Exception(
                        "Could not understand how to refresh the property '%s' of %s"
                        % (attr, obj))
        return result


class ObjectExtractor(object):

    def extract(self, obj):
        json_encoder = Encoder()
        result = {}
        for attr_name, attr in get_class_manager(obj).local_attrs.iteritems():
            if type(attr.property) is ColumnProperty:
                attr_encoded_value = json_encoder.encode(getattr(obj, attr_name,
                                                                 None))
                result[attr_name] = attr_encoded_value
            elif type(attr.property) is RelationshipProperty:
                logging.info(
                    "Processing of RelationshipProperty is not yet implemented")
        return result


class ObjectSaver(object):

    def __init__(self, session):
        self.session = session

    def save(self, obj):
        extractor = ObjectExtractor()
        attribute_refresher = ObjectAttributeRefresher()
        # json_encoder = Encoder()
        attribute_refresher.refresh(obj)
        obj_as_dict = extractor.extract(obj)

        tablename = obj.__table__.name

        if not "id" in obj_as_dict or obj_as_dict["id"] is None:
            next_id = database_driver.get_driver().next_key(tablename)
            obj_as_dict["id"] = next_id

        db_driver = database_driver.get_driver()
        db_driver.put(tablename, obj_as_dict["id"], obj_as_dict, [])
        db_driver.add_key(tablename, obj_as_dict["id"])

        return True

    def delete(self, obj):
        extractor = ObjectExtractor()
        attribute_refresher = ObjectAttributeRefresher()
        # json_encoder = Encoder()
        attribute_refresher.refresh(obj)
        obj_as_dict = extractor.extract(obj)

        tablename = obj.__table__.name

        if not "id" in obj_as_dict or obj_as_dict["id"] is None:
            next_id = database_driver.get_driver().next_key(tablename)
            obj_as_dict["id"] = next_id

        db_driver = database_driver.get_driver()
        db_driver.remove_key(tablename, obj_as_dict["id"])

        return True
