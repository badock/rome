"""Query module.

This module contains a definition of object queries.

"""

from sqlalchemy import Integer, String
import logging
from sqlalchemy.ext.declarative.api import DeclarativeMeta
from lib.rome2.core.dataformat.json import Decoder


class Query(object):

    def __init__(self, *args, **kwargs):

        self.session = kwargs.pop("__session", None)

        if "__query" in kwargs:
            self.sqlalchemy_query = kwargs["__query"]
        else:
            if self.session:
                from sqlalchemy.orm.session import Session
                temporary_session = Session()
                self.sqlalchemy_query = temporary_session.query(*args, **kwargs)
            else:
                from sqlalchemy.orm import Query as SqlAlchemyQuery
                self.sqlalchemy_query = SqlAlchemyQuery(*args, **kwargs)

    def _set_query(self, query):
        self.sqlalchemy_query = query

    def __getattr__(self, item):
        from sqlalchemy.orm import Query as SqlAlchemyQuery
        import types
        if hasattr(self.sqlalchemy_query, item):
            result = getattr(self.sqlalchemy_query, item, None)
            if isinstance(result, types.MethodType):
                def anonymous_func(*args, **kwargs):
                    call_result = result(*args, **kwargs)
                    if isinstance(call_result, SqlAlchemyQuery):
                        new_query = Query(*[], **{"__query": call_result})
                        new_query._set_query(call_result)
                        return new_query
                    return call_result
                return anonymous_func
            return result
        return super(object, self).__getattr__(item)

    def _extract_entity_class_registry(self):
        from sqlalchemy.ext.declarative.api import DeclarativeMeta
        for description in self.sqlalchemy_query.column_descriptions:
            if "entity" in description:
                declarative_meta = description["entity"]
                entity_class_registry_instance_weak = getattr(declarative_meta, "_decl_class_registry", None)
                if entity_class_registry_instance_weak is not None:
                    entity_class_registry = {}
                    for elmnt in entity_class_registry_instance_weak.values():
                        if type(elmnt) is DeclarativeMeta:
                            description = elmnt.__table__.description
                            entity_class_registry[description] = elmnt
                    return entity_class_registry
        return None

    def all(self):
        from lib.rome2.core.orm.utils import get_literal_query
        from lib.rome2.lang.sql_parser import QueryParser
        from lib.rome2.core.rows.rows import construct_rows

        sql_query = get_literal_query(self.sqlalchemy_query)
        parser = QueryParser()

        query_tree = parser.parse(sql_query)
        entity_class_registry = self._extract_entity_class_registry()

        rows = construct_rows(query_tree, entity_class_registry)

        def row_function(row, column_descriptions, decoder):
            final_row = []
            one_is_an_object = False
            for column_description in column_descriptions:
                if type(column_description["type"]) in [Integer, String]:
                    row_key = column_description["entity"].__table__.name.capitalize()
                    property_name = column_description["name"]
                    if row_key in row and property_name in row[row_key]:
                        value = row[row_key].get(column_description["name"], None)
                    if value is not None:
                        final_row += [value]
                    else:
                        logging.error("Could not understand how to get the value of '%s' with this: '%s'" % (column_description.get("expr", "??"), row))
                elif type(column_description["type"]) == DeclarativeMeta:
                    one_is_an_object = True
                    row_key = column_description["entity"].__table__.name.capitalize()
                    new_object = column_description["entity"]()
                    attribute_names = map(lambda x: x.key, list(column_description["entity"].__table__.columns))
                    for attribute_name in attribute_names:
                        value = decoder.desimplify(row[row_key].get(attribute_name, None))
                        setattr(new_object, attribute_name, value)
                    final_row += [new_object]
                else:
                    logging.error("Unsupported type: '%s'" % (column_description["type"]))
            if not one_is_an_object:
                return [final_row]
            else:
                return final_row

        decoder = Decoder()
        final_rows = map(lambda r: row_function(r, self.sqlalchemy_query.column_descriptions, decoder), rows)

        if len(self.sqlalchemy_query.column_descriptions) == 1:
            # Flatten the list
            final_rows = [item for sublist in final_rows for item in sublist]

        return final_rows
