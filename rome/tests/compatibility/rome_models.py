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
    from rome.core.session.session import Session as RomeSession

    # eng = create_engine('sqlite:///:memory:')
    # Session = sessionmaker(bind=eng)

    # session = Session()
    session = RomeSession()

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
