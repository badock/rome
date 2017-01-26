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


def get_class_registry(obj):
    return getattr(obj, "_decl_class_registry", None)


def find_entity_class(tablename, class_registry):
    for (entitity_class_name, entity_class) in class_registry.iteritems():
        if hasattr(entity_class,
                   "__table__") and entity_class.__table__.name == tablename:
            return entity_class
    return None


def find_an_association_class(property_pairs, class_registry):
    tables_count = {}
    for left, right in property_pairs:
        left_table_count = tables_count.get(left.table.name, 0)
        tables_count[left.table.name] = left_table_count + 1
        right_table_count = tables_count.get(right.table.name, 0)
        tables_count[right.table.name] = right_table_count + 1
    ranks = map(lambda k: {"key": k, "count": tables_count[k]}, tables_count)
    sorted_ranks = sorted(ranks, key=lambda v: v["count"], reverse=True)

    if len(sorted_ranks) >= 2:
        if sorted_ranks[0]["count"] != sorted_ranks[1]["count"]:
            association_tablename = sorted_ranks[0]["key"]
            return find_entity_class(association_tablename, class_registry)
    return None


def find_associations_attributes(property_pairs):
    tables_count = {}
    association_tables = {}
    for left, right in property_pairs:
        for x in [left, right]:
            table_count = tables_count.get(x.table.name, 0)
            tables_count[x.table.name] = table_count + 1
            table_attributes = association_tables.get(x.table.name, [])
            association_tables[x.table.name] = table_attributes + [{
                "local": x.name,
                "local_table": x.table.name,
                "remote": right.name
            }]
    ranks = map(lambda k: {"key": k, "count": tables_count[k]}, tables_count)
    sorted_ranks = sorted(ranks, key=lambda v: v["count"], reverse=True)

    if len(sorted_ranks) >= 2:
        max_rank = max(map(lambda r: r["count"], ranks))
        filtered_ranks = filter(lambda v: v["count"] < max_rank, sorted_ranks)

        associations_attributes = []
        for rank in filtered_ranks:
            associations_attributes += association_tables.pop(rank["key"], [])
        return associations_attributes

    return []


def find_an_identifier(obj):
    if hasattr(obj, "id"):
        identifier = getattr(obj, "id", None)
    else:
        primary_key_components = obj._sa_class_manager.mapper.primary_key
        primary_key_parts = map(lambda x: "%s" % (getattr(obj, x.name)), primary_key_components)
        if None in primary_key_parts:
            return None
        identifier = "_".join(primary_key_parts)
    return identifier


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
            attr_value = getattr(obj, attr_name, None)
            if attr_value is None:
                continue
            elements = attr_value if hasattr(attr_value, "__len__") else [attr_value]
            for element in elements:
                element_value = getattr(element, right.name, None)
                obj_attr_value = getattr(obj, left.name, None)
                if element_value is None or element_value != obj_attr_value:
                    setattr(element, right.name, obj_attr_value)
        return True

    def _generate_query(self, entity_class, additional_expression):
        query = Query(entity_class).filter(additional_expression)
        return query

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
                    class_registry = get_class_registry(obj)
                    right_entity = find_entity_class(right.table.name, class_registry)
                    query = self._generate_query(right_entity, right.__eq__(left_value))
                    relationship_field = LazyRelationship(query, right_entity, many=False)
                    setattr(obj, attr_name, relationship_field)
            elif right_value is not None:
                right_value_field_value = getattr(right_value, right.name, None)
                if right_value_field_value:
                    setattr(obj, left.name, right_value_field_value)
        return True

    def refresh_many_to_many(self, obj, attr_name, attr):
        """
        Refresh a many-to-many relationship of a python object.
        :param obj: a python object
        :param attr_name: name of the relationship field
        :param attr: relationship object
        :return: a boolean which is True if the refresh worked
        """
        association_class = find_an_association_class(attr.property.local_remote_pairs,
                                                      get_class_registry(obj))

        if association_class is not None:
            from rome.core.session.session import Session as RomeSession
            association_attributes = find_associations_attributes(attr.property.local_remote_pairs)
            tmp_session = RomeSession()

            existing_associations = tmp_session.query(association_class).all()
            for association in existing_associations:
                pass

            attribute_list = getattr(obj, attr_name)

            # Prevent to load a lazy relationship by mistake
            from rome.core.utils import LazyRelationship
            if type(attribute_list) is LazyRelationship:
                if not attribute_list.is_loaded:
                    return

            if len(attribute_list) == 0 and len(existing_associations) > 0:
                # The 'attribute_list' may not have been populated: the following code will check if
                # there are some objects (from  'existing_associations' variable) that should be
                # inserted in the attribute_list property.
                local_association_attribute_candidates = \
                    filter(lambda x: x["local_table"] == obj.__table__.name, association_attributes)
                remote_association_attribute_candidates = \
                    filter(lambda x: x["remote"] != obj.__table__.name, association_attributes)
                if len(local_association_attribute_candidates) == 0:
                    return
                local_attr = local_association_attribute_candidates[0]
                remote_attr = remote_association_attribute_candidates[0]
                matching_assocs = filter(lambda x:
                                         getattr(obj, local_attr["local"])
                                         == getattr(x, local_attr["remote"]),
                                         existing_associations)

                remote_values = map(lambda x: getattr(x, remote_attr["remote"]),
                                    matching_assocs)
                class_registry = get_class_registry(obj)
                entity = find_entity_class(remote_attr["local_table"], class_registry)
                remote_attribute = getattr(entity, remote_attr["local"])
                query = tmp_session.query(entity).filter(remote_attribute.in_(remote_values))
                lazy_relationship_field = LazyRelationship(query, entity)
                obj.__dict__[attr_name] = lazy_relationship_field
            else:
                # The 'attribute_list' seems to have been populated correctly: will check if some
                # association objects are not in the 'attribute_list' property.
                for item in getattr(obj, attr_name):
                    association_complete = True
                    new_association = association_class()
                    for attribute in association_attributes:
                        target = obj if attribute["local_table"] == obj.__table__.name else item
                        v = getattr(target, attribute["local"])
                        if v is not None:
                            setattr(new_association, attribute["remote"], v)
                        else:
                            association_complete = False
                    if association_complete:
                        tmp_session.add(new_association)

            tmp_session.flush()
        return True

    def refresh(self, obj):
        """
        Refresh relationships objects according to foreign keys (and vice versa).
        :param obj: a python object
        :return: a boolean which is True if the object has been successfully refreshed
        """
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
        return True


class ObjectExtractor(object):

    def extract(self, obj):
        """
        Extract the dictionary representation of a SQLAlchemy entity object.
        :param obj: an entity object
        :return: a python dictionary
        """
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
        """
        Save in database an SQLAlchemy entity object.
        :param obj: an entity object
        :return: a boolean which is True if the object has been successfully saved in database
        """
        extractor = ObjectExtractor()
        attribute_refresher = ObjectAttributeRefresher()
        # json_encoder = Encoder()
        attribute_refresher.refresh(obj)
        obj_as_dict = extractor.extract(obj)

        tablename = str(obj.__table__.name)

        if hasattr(obj, "id"):
            if "id" not in obj_as_dict or obj_as_dict["id"] is None:
                next_id = database_driver.get_driver().next_key(tablename)
                key_to_use = next_id
            else:
                key_to_use = obj.id
        else:
            key_to_use = find_an_identifier(obj)

        obj_as_dict["id"] = key_to_use

        db_driver = database_driver.get_driver()
        db_driver.put(tablename, key_to_use, obj_as_dict, [])
        db_driver.add_key(tablename, key_to_use)

        # Set the ID of the obj parameters
        if hasattr(obj, "id") and obj.id is None:
            obj.id = obj_as_dict["id"]
            attribute_refresher.refresh(obj)

        return True

    def delete(self, obj):
        """
        Delete from database an SQLAlchemy entity object.
        :param obj: an entity object
        :return: a boolean which is True if the object has been successfully deleted from database
        """
        extractor = ObjectExtractor()
        attribute_refresher = ObjectAttributeRefresher()
        # json_encoder = Encoder()
        attribute_refresher.refresh(obj)
        obj_as_dict = extractor.extract(obj)

        tablename = obj.__table__.name

        if hasattr(obj, "id"):
            if "id" not in obj_as_dict or obj_as_dict["id"] is None:
                next_id = database_driver.get_driver().next_key(tablename)
                key_to_use = next_id
            else:
                key_to_use = obj.id
        else:
            key_to_use = find_an_identifier(obj)

        obj_as_dict["id"] = key_to_use

        db_driver = database_driver.get_driver()
        db_driver.remove_key(tablename, obj_as_dict["id"])

        return True
