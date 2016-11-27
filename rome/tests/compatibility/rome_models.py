from rome.core.session.session import Session
from sqlalchemy_models import Author, Book


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
    from rome.core.session.session import Session as RomeSession
    session = RomeSession()
    query = session.query(*args, **kwargs)

    return query
