from sqlalchemy.orm import joinedload, joinedload_all, subqueryload, subqueryload_all

import rome_models
import sqlalchemy_models
from test.compatibility.utils import get_literal_query

def init_mock_objects(models_module):
    models_module.drop_tables()
    models_module.create_tables()
    models_module.init_objects()


""" It should implement the behaviour depicted in
http://docs.sqlalchemy.org/en/latest/orm/loading_relationships.html """


def testing_join_1(query_module):
    models_module = sqlalchemy_models
    query = query_module.get_query(models_module.Author).join(models_module.Book)
    if query_module is sqlalchemy_models:
        print(query.column_descriptions)
        print get_literal_query(query)
    extract_column = lambda x: getattr(x, "__tablename__", str(x))
    extract_row = lambda x: map(lambda y: extract_column(y), x) if hasattr(x, "__iter__") else extract_column(x)

    rows = query.all()
    result = map(extract_row, rows)
    return str(result)


def testing_join_2(query_module):
    models_module = sqlalchemy_models
    query = query_module.get_query(models_module.Author, models_module.Book).join(models_module.Book)
    if query_module is sqlalchemy_models:
        print(query.column_descriptions)
        print get_literal_query(query)
    extract_column = lambda x: getattr(x, "__tablename__", str(x))
    extract_row = lambda x: map(lambda y: extract_column(y), x) if hasattr(x, "__iter__") else extract_column(x)

    rows = query.all()
    result = map(extract_row, rows)
    return str(result)


def testing_join_3(query_module):
    models_module = sqlalchemy_models
    query = query_module.get_query(models_module.Author).join(models_module.Author.books)
    if query_module is sqlalchemy_models:
        print(query.column_descriptions)
        print get_literal_query(query)
    extract_column = lambda x: getattr(x, "__tablename__", str(x))
    extract_row = lambda x: map(lambda y: extract_column(y), x) if hasattr(x, "__iter__") else extract_column(x)

    rows = query.all()
    result = map(extract_row, rows)
    return str(result)


def testing_join_4(query_module):
    models_module = sqlalchemy_models
    query = models_module.get_query(models_module.Author).join(models_module.Author.books).options(
        joinedload(models_module.Author.books))
    if query_module is sqlalchemy_models:
        print(query.column_descriptions)
        print get_literal_query(query)
    extract_column = lambda x: getattr(x, "__tablename__", str(x))
    extract_row = lambda x: map(lambda y: extract_column(y), x) if hasattr(x, "__iter__") else extract_column(x)

    rows = query.all()
    result = map(extract_row, rows)
    return str(result)


def testing_join_5(query_module):
    models_module = sqlalchemy_models
    query = query_module.get_query(models_module.Author).join(models_module.Author.books).options(
        joinedload(models_module.Author.books)).filter(models_module.Author.name == 'Author3').filter(
        models_module.Book.title == 'Book3_5')
    if query_module is sqlalchemy_models:
        print(query.column_descriptions)
        print get_literal_query(query)
    extract_column = lambda x: getattr(x, "__tablename__", str(x))
    extract_row = lambda x: map(lambda y: extract_column(y), x) if hasattr(x, "__iter__") else extract_column(x)

    rows = query.all()
    result = map(extract_row, rows)
    return str(result)


def testing_join_6(query_module):
    models_module = sqlalchemy_models
    query = query_module.get_query(models_module.Author, models_module.Author.name).join(models_module.Author.books).options(
        subqueryload(models_module.Author.books)).filter(models_module.Author.name == 'Author3').filter(
        models_module.Book.title == 'Book3_5')
    if query_module is sqlalchemy_models:
        print(query.column_descriptions)
        print(get_literal_query(query))
    extract_column = lambda x: getattr(x, "__tablename__", str(x))
    extract_row = lambda x: map(lambda y: extract_column(y), x) if hasattr(x, "__iter__") else extract_column(x)

    rows = query.all()
    result = map(extract_row, rows)
    return str(result)


def compare(function, model_a, model_b):
    result_a = function(model_a)
    result_b = function(model_b)

    if result_a != result_b:
        print("ERROR: %s != %s" % (result_a, result_b))
    else:
        print("Success!")


if __name__ == '__main__':
    # Init objects
    init_mock_objects(sqlalchemy_models)
    init_mock_objects(rome_models)

    # Basic join
    compare(testing_join_1, sqlalchemy_models, rome_models)
    compare(testing_join_2, sqlalchemy_models, rome_models)
    compare(testing_join_3, sqlalchemy_models, rome_models)
    compare(testing_join_4, sqlalchemy_models, rome_models)
    compare(testing_join_5, sqlalchemy_models, rome_models)
    compare(testing_join_6, sqlalchemy_models, rome_models)

