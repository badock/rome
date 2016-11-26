from oslo_config import cfg
from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import orm
from sqlalchemy.dialects.mysql import MEDIUMTEXT
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.sqltypes import *

from lib.rome.core.models import Entity as NovaBase
from lib.rome.core.models import global_scope
from lib.rome.core.orm.query import Query
from lib.rome2.core.session.session import Session

from sqlalchemy_models import Author, Book

CONF = cfg.CONF
BASE = declarative_base(constructor=NovaBase.__init__)


def MediumText():
    return Text().with_variant(MEDIUMTEXT(), 'mysql')


# @global_scope
# class AuthorRome(BASE, NovaBase):
#     __tablename__ = "AuthorsRome"
#
#     id = Column(Integer, primary_key=True)
#     name = Column(String)
#
#
# @global_scope
# class BookRome(BASE, NovaBase):
#     __tablename__ = "Books"
#
#     id = Column(Integer, primary_key=True)
#     title = Column(String)
#     price = Column(Integer)
#     author_id = Column(Integer, ForeignKey("AuthorsRome.id"))
#
#     Author = orm.relationship(AuthorRome, backref=orm.backref('books'), foreign_keys=author_id)


def drop_tables():
    drop_table_session = Session()
    for obj in get_query(Book).all():
        drop_table_session.delete(obj)
    for obj in get_query(Author).all():
        drop_table_session.delete(obj)
    drop_table_session.commit()


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


def get_query(*args, **kwargs):
    # from lib.rome2.core.orm.query import Query as RomeQuery
    # query = RomeQuery(*args, **kwargs)

    from lib.rome2.core.session.session import Session as RomeSession
    session = RomeSession()
    query = session.query(*args, **kwargs)

    return query


if __name__ == "__main__":

    from sqlalchemy.orm import subqueryload

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

    from sqlalchemy.orm.session import Session
    from lib.rome2.core.session.session import Session as RomeSession

    # eng = create_engine('sqlite:///:memory:')
    # Session = sessionmaker(bind=eng)

    # session = Session()
    session = RomeSession()
    from lib.rome2.core.orm.query import Query as RomeQuery

    # query = RomeQuery(sqlalchemy_models.Author, sqlalchemy_models.Author.name)\
    #             .join(sqlalchemy_models.Author.books)\
    #             .options(subqueryload(sqlalchemy_models.Author.books))\
    #             .filter(sqlalchemy_models.Author.name == 'Author3')\
    #             .filter(sqlalchemy_models.Book.title == 'Book3_5')

    query = session.query(sqlalchemy_models.Author, sqlalchemy_models.Author.name)\
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
