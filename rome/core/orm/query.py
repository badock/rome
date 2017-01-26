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
        self.query_tree = None
        self.entity_class_registry = None
        self._autoflush = True

        # Sometimes the query must return attributes rather than objects. The following block
        # is in charge of finding which attributes should be returned by the query.
        attributes = []
        for arg in args:
            from sqlalchemy.orm.attributes import InstrumentedAttribute
            if type(arg) is InstrumentedAttribute:
                attributes += [arg]
        self.required_attributes = attributes

        # Create the SQLAlchemy query
        if "__query" in kwargs:
            self.sa_query = kwargs["__query"]
        else:
            if self.session:
                from sqlalchemy.orm.session import Session
                temporary_session = Session()
                self.sa_query = temporary_session.query(*args, **kwargs)
            else:
                from sqlalchemy.orm import Query as SqlAlchemyQuery
                from rome.core.session.session import Session as RomeSession
                # self.sa_query = SqlAlchemyQuery(*args, **kwargs)
                entities = filter(lambda arg: type(arg) is not RomeSession and arg is not None, args)
                session_candidates = filter(lambda arg: type(arg) is RomeSession, args)
                if len(session_candidates) > 0:
                    session_candidate = session_candidates[0]
                else:
                    session_candidate = None
                self.session = session_candidate
                if len(attributes) == 0:
                    self.sa_query = SqlAlchemyQuery(entities, session=session_candidate)
                else:
                    self.sa_query = SqlAlchemyQuery(attributes, session=session_candidate)

    def set_sa_query(self, query):
        """
        Set the SQLAlchemy query
        :param query: an SQLAlchemy query
        """
        self.sa_query = query

    def set_query_tree(self, query_tree):
        """
        Set the query_tree
        :param query: a instance of QueryParserResult
        """
        self.query_tree = query_tree

    def set_entity_class_registry(self, entity_class_registry):
        """
        Set the "entity_class_registry" field
        :param query: a instance of EntityClassRegistry
        """
        self.entity_class_registry = entity_class_registry

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
                        new_query.session = self.session
                        new_query.required_attributes = self.required_attributes
                        return new_query
                    return call_result

                return anonymous_func
            return result
        # return super(object, self).__getattr__(item)
        return getattr(super(object, self), item)

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

    def matching_objects(self, filter_deleted):
        """
        Execute the query, and return its result as rows
        :param filter_deleted: a boolean. When filter_deleted is True, matching objects that have
        been soft_deleted are filtered
        :return: a list of tuples (can be objects/values or list of objects values)
        """
        from rome.core.orm.utils import get_literal_query
        from rome.lang.sql_parser import QueryParser
        from rome.core.rows.rows import construct_rows

        if self._autoflush:
            if self.session is not None:
                self.session.commit()

        if not self.query_tree:
            sql_query = get_literal_query(self.sa_query)
            parser = QueryParser()
            query_tree = parser.parse(sql_query)
        else:
            query_tree = self.query_tree

        if not self.entity_class_registry:
            self.entity_class_registry = self._extract_entity_class_registry()
        entity_class_registry = self.entity_class_registry

        # Collecting variables of sub queries
        subqueries_variables = {}
        for (variable_name, sub_query_tree) in query_tree.variables.iteritems():
            sub_query = Query()
            sub_query.set_query_tree(sub_query_tree)
            sub_query.set_entity_class_registry(entity_class_registry)
            result = sub_query.all()
            subqueries_variables[variable_name] = result

        rows = construct_rows(query_tree,
                              entity_class_registry,
                              filter_deleted=filter_deleted,
                              subqueries_variables= subqueries_variables)

        def row_function(row, column_descriptions, decoder):
            from rome.core.session.utils import ObjectAttributeRefresher
            final_row = []
            one_is_an_object = False
            object_attribute_refresher = ObjectAttributeRefresher()
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
                    row_key = column_description["entity"].__table__.name
                    new_object = column_description["entity"]()
                    attribute_names = map(lambda x: x.key, list(
                        column_description["entity"].__table__.columns))
                    for attribute_name in attribute_names:
                        value = decoder.decode(row[row_key].get(attribute_name,
                                                                None))
                        setattr(new_object, attribute_name, value)

                    if "___version_number" in row[row_key]:
                        setattr(new_object, "___version_number", row[row_key]["___version_number"])

                    object_attribute_refresher.refresh(new_object)
                    final_row += [new_object]
                else:
                    logging.error("Unsupported type: '%s'" %
                                  (column_description["type"]))
            if not one_is_an_object:
                return [final_row]
            else:
                return final_row

        def row_function_subquery(row, attributes, decoder):
            result = []
            for attribute in attributes:
                tablename = attribute.split(".")[0]
                attribute_name = attribute.split(".")[1]
                result += [row[tablename][attribute_name]]
            return result

        decoder = Decoder()

        if len(self.sa_query.column_descriptions) > 0:
            final_rows = map(lambda r: row_function(
                r, self.sa_query.column_descriptions, decoder), rows)
        else:
            final_rows = map(lambda r: row_function_subquery(
                r, self.query_tree.attributes, decoder), rows)

        if len(self.sa_query.column_descriptions) <= 1:
            # Flatten the list
            final_rows = [item for sublist in final_rows for item in sublist]

        # Add watcher on objects
        if self.session is not None:
            for obj in final_rows:
                if hasattr(obj, "id"):
                    self.session.watch(obj)

        return final_rows

    def all(self, filter_deleted=True):
        """
        Execute the query, and return its result as rows
        :param filter_deleted: a boolean. When filter_deleted is True, matching objects that have
        been soft_deleted are filtered
        :return: a list of tuples (can be objects/values or list of objects values)
        """
        objects = self.matching_objects(filter_deleted=filter_deleted)
        return objects

    def first(self, filter_deleted=True):
        """
        Executes the query and returns the first matching row.
        :param filter_deleted: a boolean. When filter_deleted is True, matching objects that have
        been soft_deleted are filtered
        :return: a single tuple (can be objects/values or list of objects values) if a value could
        be found. None is returned if no value can be found.
        """
        objects = self.matching_objects(filter_deleted=filter_deleted)

        if len(objects) > 0:
            value = objects[0]
            if self.session is not None:
                if hasattr(value, "id"):
                    self.session.watch(value)
            return value
        else:
            return None

    def count(self):
        """
        Executes the query and returns the number of matching rows.
        :return: an int corresponding of the number of rows matching the request.
        """
        objects = self.all()
        return len(objects)

    def delete(self):
        from rome.core.session.session import Session
        temporary_session = Session()
        objects = self.matching_objects(filter_deleted=False)
        for obj in objects:
            temporary_session.delete(obj)
        temporary_session.flush()
        return len(objects)

    def soft_delete(self, synchronize_session='evaluate'):
        import datetime
        session = self.session
        objects = self.all(filter_deleted=False)
        for obj in objects:
            """Mark this object as deleted."""
            obj["deleted"] = obj.id
            obj["deleted_at"] = datetime.datetime.utcnow()
            session.add(obj)
        session.flush()
        return len(objects)

    def __iter__(self):
        return iter(self.all())

    def update(self, values, synchronize_session='evaluate', update_args=None):
        matching_objects = self.matching_objects(filter_deleted=False)
        session = self.session
        for obj in matching_objects:
            for key, value in values.iteritems():
                setattr(obj, key, value)
            session.add(obj)
        session.flush()
        return len(matching_objects)

    def update_on_match(self, specimen, surrogate_key, values, **kw):
        matching_objects = self.matching_objects(filter_deleted=False)
        for matching_object in matching_objects:
            matching = False
            if matching_object[surrogate_key] == specimen[surrogate_key]:
                    matching = True
            if matching:
                session = self.session
                for k, v in values.iteritems():
                    matching_object[k] = v
                session.add(matching_object)
                session.flush()
                return matching_object
        return None

    def one(self):
        matching_objects = self.matching_objects(filter_deleted=False)
        if len(matching_objects) == 0:
            from sqlalchemy.orm.exc import NoResultFound
            raise NoResultFound()
        if len(matching_objects) > 1:
            from sqlalchemy.orm.exc import MultipleResultsFound
            raise MultipleResultsFound()
        return matching_objects[0]

    def with_lockmode(self, mode):
        return self

