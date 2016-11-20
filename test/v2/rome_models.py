from oslo_config import cfg

from lib.rome.core.models import Entity as NovaBase
from lib.rome.core.models import global_scope
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy.dialects.mysql import MEDIUMTEXT
from sqlalchemy import orm
from sqlalchemy import ForeignKey, Text
from lib.rome.core.session.session import Session

from lib.rome.core.orm.query import Query
from sqlalchemy.sql.sqltypes import *
from sqlalchemy.ext.declarative.api import DeclarativeMeta
from lib.rome2.core.dataformat.json import Decoder

import logging

CONF = cfg.CONF
BASE = declarative_base(constructor=NovaBase.__init__)


def MediumText():
    return Text().with_variant(MEDIUMTEXT(), 'mysql')


@global_scope
class AuthorRome(BASE, NovaBase):
    __tablename__ = "Authors"

    id = Column(Integer, primary_key=True)
    name = Column(String)


@global_scope
class BookRome(BASE, NovaBase):
    __tablename__ = "Books"

    id = Column(Integer, primary_key=True)
    title = Column(String)
    price = Column(Integer)
    author_id = Column(Integer, ForeignKey("Authors.id"))

    Author = orm.relationship(AuthorRome, backref=orm.backref('books'), foreign_keys=author_id)


def drop_tables():
    for obj in Query(BookRome).all():
        obj.delete()
    for obj in Query(AuthorRome).all():
        obj.delete()


def create_tables():
    # Base.metadata.create_all()
    pass


ses = Session()


def init_objects():

    next_author_id = 1
    next_book_id = 1
    for i in range(1, 4):
        author_id = next_author_id
        author = AuthorRome()
        author.id = author_id
        author.name = 'Author%s' % (author_id)
        ses.add_all(
           [author])
        if i != 1:
            for j in range(1, 5):
                book_id = next_book_id
                book = BookRome()
                book.id = book_id
                book.title = 'Book%s_%s' % (i, book_id)
                book.price = 200
                book.author_id = author_id
                ses.add_all(
                   [book])
                next_book_id += 1
        next_author_id += 1

    ses.commit()

# def init_objects():
#     next_author_id = 1
#     next_book_id = 1
#     for i in range(1, 4):
#         author_id = next_author_id
#         author = Author()
#         author.id = author_id
#         author.name = 'Author%s' % (author_id)
#         author.save()
#         if i != 1:
#             for j in range(1, 5):
#                 book_id = next_book_id
#                 book = Book()
#                 book.id = book_id
#                 book.title = 'Book%s_%s' % (i, book_id)
#                 book.price = 200
#                 book.author_id = author_id
#                 book.save()
#                 next_book_id += 1
#         next_author_id += 1


class V2Query(object):

    def __init__(self, *args, **kwargs):
        from sqlalchemy.orm import Query as SqlAlchemyQuery
        if "__query" in kwargs:
            self.sqlalchemy_query = kwargs["__query"]
        else:
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
                        new_query = V2Query(*[], **{"__query": call_result})
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
        from utils import get_literal_query
        from lib.rome2.lang.sql_parser import QueryParser
        from lib.rome2.core.rows.rows import construct_rows

        sql_query = get_literal_query(self.sqlalchemy_query)
        parser = QueryParser()

        query_tree = parser.parse(sql_query)
        entity_class_registry = self._extract_entity_class_registry()

        rows = construct_rows(query_tree, entity_class_registry)

        def row_function(row, column_descriptions, decoder):
            final_row = []
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
                    row_key = column_description["entity"].__table__.name.capitalize()
                    new_object = column_description["entity"]()
                    attribute_names = map(lambda x: x.key, list(column_description["entity"].__table__.columns))
                    for attribute_name in attribute_names:
                        value = decoder.desimplify(row[row_key].get(attribute_name, None))
                        setattr(new_object, attribute_name, value)
                    final_row += [new_object]
                else:
                    logging.error("Unsupported type: '%s'" % (column_description["type"]))
            return final_row

        decoder = Decoder()
        final_rows = map(lambda r: row_function(r, self.sqlalchemy_query.column_descriptions, decoder), rows)

        if len(self.sqlalchemy_query.column_descriptions) == 1:
            # Flatten the list
            final_rows = [item for sublist in final_rows for item in sublist]

        return final_rows


def get_query(*args, **kwargs):
    query_v2 = V2Query(*args, **kwargs)
    return query_v2


if __name__ == "__main__":

    from sqlalchemy.orm import joinedload, joinedload_all, subqueryload, subqueryload_all

    # Init data
    drop_tables()
    create_tables()
    init_objects()

    # # Create a sample query
    # # query = V2Query(Author).join(Book)
    # query = V2Query(Author).join(Author.books).options(joinedload(Author.books)).filter(Author.name == 'Author3').filter(Book.title == 'Book3_5')
    # result = query.all()
    # print(result)

    # Create a sample query
    # query = V2Query(Author).join(Book)
    import sqlalchemy_models

    # query = V2Query(Author).join(Author.books).options(
    #     joinedload(Author.books)).filter(Author.name == 'Author3').filter(Book.title == 'Book3_5')

    query = V2Query(sqlalchemy_models.Author)\
                .join(sqlalchemy_models.Author.books)\
                .options(subqueryload(sqlalchemy_models.Author.books))\
                .filter(sqlalchemy_models.Author.name == 'Author3')\
                .filter(sqlalchemy_models.Book.title == 'Book3_5')

    # query = V2Query(sqlalchemy_models.Author)\
    #             .join(sqlalchemy_models.Author.books)\
    #             .filter(sqlalchemy_models.Author.name == 'Author3')\
    #             .filter(sqlalchemy_models.Book.title == 'Book3_5')

    result = query.all()
    print(result)
