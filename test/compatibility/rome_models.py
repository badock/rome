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

CONF = cfg.CONF
BASE = declarative_base(constructor=NovaBase.__init__)


def MediumText():
    return Text().with_variant(MEDIUMTEXT(), 'mysql')


@global_scope
class Author(BASE, NovaBase):
    __tablename__ = "Authors"

    id = Column(Integer, primary_key=True)
    name = Column(String)



@global_scope
class Book(BASE, NovaBase):
    __tablename__ = "Books"

    id = Column(Integer, primary_key=True)
    title = Column(String)
    price = Column(Integer)
    author_id = Column(Integer, ForeignKey("Authors.id"))

    Author = orm.relationship(Author, backref=orm.backref('books'), foreign_keys=author_id)


def drop_tables():
    for obj in Query(Book).all():
        obj.delete()
    for obj in Query(Author).all():
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
        author = Author()
        author.id = author_id
        author.name = 'Author%s' % (author_id)
        ses.add_all(
           [author])
        if i != 1:
            for j in range(1, 5):
                book_id = next_book_id
                book = Book()
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


def get_query(*args, **kwargs):
    return Query(*args, **kwargs)
