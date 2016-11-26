from lib.rome2.core.dataformat.json import Decoder, Encoder
from lib.rome2.core.utils import LazyRelationship
from sqlalchemy.orm.properties import ColumnProperty, RelationshipProperty
from sqlalchemy.orm.interfaces import ONETOMANY, MANYTOONE, MANYTOMANY
from lib.rome2.core.orm.query import Query
import lib.rome.driver.database_driver as database_driver
import logging


def get_class_manager(obj):
    return getattr(obj, "_sa_class_manager", None)


def recursive_getattr(obj, key, default=None):
    sub_keys = key.split(".")
    current_key = sub_keys[0]
    if hasattr(obj, current_key):
        current_object = getattr(obj, current_key)
        if len(current_key) == 1:
            return current_object
        else:
            return recursive_getattr(current_object, ".".join(sub_keys[1:]), default)
    else:
        if default:
            return default
        else:
            raise Exception("Could not find property '%s' in %s" % (current_key, obj))


class ObjectAttributeRefresher(object):

    def refresh_one_to_many(self, obj, attr_name, attr):
        for l, r in attr.property.local_remote_pairs:
            l_value = getattr(obj, attr_name, None)
            if l_value:
                for e in l_value:
                    e_value = getattr(e, r.name, None)
                    if e_value is None or e_value != l_value:
                        setattr(e, r.name, e_value)
        return True

    def _generate_query(self, attr, additional_expression):
        if len(attr.prop._reverse_property) > 0:
            reverse_property = list(attr.prop._reverse_property)[0]
            entity_class = reverse_property.parent.entity
            query = Query(entity_class).filter(attr.expression).filter(additional_expression)
            return (entity_class, query)
        else:
            logging.info("Could not generate a query with those parameters: %s, %s" % (attr, additional_expression))
            raise Exception("Could not generate a query with those parameters: %s, %s" % (attr, additional_expression))

    def refresh_many_to_one(self, obj, attr_name, attr):
        for l, r in attr.property.local_remote_pairs:
            l_value = getattr(obj, l.name, None)
            r_value = getattr(obj, attr_name, None)
            if l_value is not None:
                if r_value is None:
                    (entity_class, query) = self._generate_query(attr, r.__eq__(l_value))
                    relationship_field = LazyRelationship(query, entity_class, many=False)
                    setattr(obj, attr_name, relationship_field)
            elif r_value is not None:
                r_value_field_value = getattr(r_value, r.name, None)
                if r_value_field_value:
                    new_l_value = getattr(obj, l.name, r_value_field_value)
                    setattr(obj, l.name, new_l_value)
        return True

    def refresh_many_to_many(self, obj, attr_name, attr):
        for l, r in attr.property.local_remote_pairs:
            l_value = getattr(obj, attr_name, None)
            if l_value:
                for e in l_value:
                    e_value = getattr(e, r.name, None)
                    if e_value is None or e_value != l_value:
                        setattr(e, r.name, e_value)
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
                    logging.error("Could not understand how to refresh the property '%s' of %s" % (attr, obj))
                    raise Exception("Could not understand how to refresh the property '%s' of %s" % (attr, obj))
        return result


class ObjectExtractor(object):

    def extract(self, obj):
        json_encoder = Encoder()
        result = {}
        for attr_name, attr in get_class_manager(obj).local_attrs.iteritems():
            if type(attr.property) is ColumnProperty:
                attr_encoded_value = json_encoder.simplify(getattr(obj, attr_name, None))
                result[attr_name] = attr_encoded_value
            elif type(attr.property) is RelationshipProperty:
                logging.info("Processing of RelationshipProperty is not yet implemented")
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

