from sqlalchemy import Column, Integer, String
from sqlalchemy import create_engine, ForeignKey
from sqlalchemy import orm
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship

eng = create_engine('sqlite:///:memory:')

Base = declarative_base()


class Author(Base):
    __tablename__ = "Authors"

    id = Column(Integer, primary_key=True)
    name = Column(String)


class Book(Base):
    __tablename__ = "Books"

    id = Column(Integer, primary_key=True)

    title = Column(String)
    price = Column(Integer)
    author_id = Column(Integer, ForeignKey("Authors.id"))

    Author = relationship(Author,
                          backref=orm.backref('books'),
                          foreign_keys=author_id)


Base.metadata.bind = eng


def drop_tables():
    try:
        Book.__table__.drop()
    except:
        print("Table Book already dropped")
        pass

    try:
        Author.__table__.drop()
    except:
        print("Table Author already dropped")
        pass


def create_tables():
    Base.metadata.create_all()


Base.metadata.create_all()

Session = sessionmaker(bind=eng)
ses = Session()


def init_objects():

    next_author_id = 1
    next_book_id = 1
    for i in range(1, 4):
        author_id = next_author_id
        author = Author()
        author.id = author_id
        author.name = 'Author%s' % (author_id)
        ses.add_all([author])
        if i != 1:
            for j in range(1, 5):
                book_id = next_book_id
                book = Book()
                book.id = book_id
                book.title = 'Book%s_%s' % (i, book_id)
                book.price = 200
                book.author_id = author_id
                ses.add_all([book])
                next_book_id += 1
        next_author_id += 1
    ses.commit()


def get_query(*args, **kwargs):
    session = Session()
    return session.query(*args, **kwargs)
