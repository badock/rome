"""Query module.

This module contains a definition of object queries.

"""

import logging

from rome.core.dataformat.json import Decoder
from sqlalchemy import Integer, String
from sqlalchemy.ext.declarative.api import DeclarativeMeta


class Query(object):

    def __init__(self, *args, **kwargs):

        self.session = kwargs.pop("__session", None)

        if "__query" in kwargs:
            self.sa_query = kwargs["__query"]
        else:
            if self.session:
                from sqlalchemy.orm.session import Session
                temporary_session = Session()
                self.sa_query = temporary_session.query(*args, **kwargs)
            else:
                from sqlalchemy.orm import Query as SqlAlchemyQuery
                self.sa_query = SqlAlchemyQuery(*args, **kwargs)

    def set_sa_query(self, query):
        """
        Set the SQLAlchemy query
        :param query: an SQLAlchemy query
        """
        self.sa_query = query

    def __getattr__(self, item):
        from sqlalchemy.orm import Query as SqlAlchemyQuery
        import types
        if hasattr(self.sa_query, item):
            result = getattr(self.sa_query, item, None)
            if isinstance(result, types.MethodType):

                def anonymous_func(*args, **kwargs):
                    call_result = result(*args, **kwargs)
                    if isinstance(call_result, SqlAlchemyQuery):
                        new_query = Query(*[], **{"__query": call_result})
                        new_query.set_sa_query(call_result)
                        return new_query
                    return call_result

                return anonymous_func
            return result
        return super(object, self).__getattr__(item)

    def _extract_entity_class_registry(self):
        """
        Extract an entity class registry from one of the models of the inner SQLAlchemy query. This
        result of this function is used by several SQLAlchemy components during the extraction of
        the SQL query from a SQLAlchemy query.
        :return: An entity class registry object if one could be found. None in case no entity class
        registry could be found
        """
        for description in self.sa_query.column_descriptions:
            if "entity" in description:
                declarative_meta = description["entity"]
                _class_registry = getattr(
                    declarative_meta, "_decl_class_registry", None)
                if _class_registry is not None:
                    entity_class_registry = {}
                    for elmnt in _class_registry.values():
                        if type(elmnt) is DeclarativeMeta:
                            description = elmnt.__table__.description
                            entity_class_registry[description] = elmnt
                    return entity_class_registry
        return None

    def all(self):
        """
        Execute the query, and return its result as rows
        :return: a list of tuples (can be objects/values or list of objects values)
        """
        from rome.core.orm.utils import get_literal_query
        from rome.lang.sql_parser import QueryParser
        from rome.core.rows.rows import construct_rows

        sql_query = get_literal_query(self.sa_query)
        parser = QueryParser()

        query_tree = parser.parse(sql_query)
        entity_class_registry = self._extract_entity_class_registry()

        rows = construct_rows(query_tree, entity_class_registry)

        def row_function(row, column_descriptions, decoder):
            final_row = []
            one_is_an_object = False
            for column_description in column_descriptions:
                if type(column_description["type"]) in [Integer, String]:
                    row_key = column_description["entity"].__table__.name.capitalize(
                    )
                    property_name = column_description["name"]
                    value = None
                    if row_key in row and property_name in row[row_key]:
                        value = row[row_key].get(property_name, None)
                    else:
                        # It seems that we are parsing the result of a function call
                        column_description_expr = column_description.get("expr",
                                                                         None)
                        if column_description_expr is not None:
                            property_name = str(column_description_expr)
                            value = row.get(property_name, None)
                    if value is not None:
                        final_row += [value]
                    else:
                        logging.error(
                            "Could not understand how to get the value of '%s' with this: '%s'"
                            % (column_description.get("expr", "??"), row))
                elif type(column_description["type"]) == DeclarativeMeta:
                    one_is_an_object = True
                    row_key = column_description["entity"].__table__.name.capitalize(
                    )
                    new_object = column_description["entity"]()
                    attribute_names = map(lambda x: x.key, list(
                        column_description["entity"].__table__.columns))
                    for attribute_name in attribute_names:
                        value = decoder.decode(row[row_key].get(attribute_name,
                                                                None))
                        setattr(new_object, attribute_name, value)
                    final_row += [new_object]
                else:
                    logging.error("Unsupported type: '%s'" %
                                  (column_description["type"]))
            if not one_is_an_object:
                return [final_row]
            else:
                return final_row

        decoder = Decoder()
        final_rows = map(lambda r: row_function(
            r, self.sa_query.column_descriptions, decoder), rows)

        if len(self.sa_query.column_descriptions) == 1:
            # Flatten the list
            final_rows = [item for sublist in final_rows for item in sublist]

        return final_rows

    def first(self):
        """
        Executes the query and returns the first matching row.
        :return: a single tuple (can be objects/values or list of objects values) if a value could
        be found. None is returned if no value can be found.
        """
        objects = self.all()
        if len(objects) > 0:
            return objects[0]
        else:
            return None
