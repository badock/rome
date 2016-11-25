import logging
import unittest

from sqlalchemy.orm import joinedload, subqueryload

import rome_models
import sqlalchemy_models


def init_mock_objects(models_module):
    models_module.drop_tables()
    models_module.create_tables()
    models_module.init_objects()


def extract_column(x):
    return getattr(x, "__tablename__", str(x))


def extract_row(x):
    if hasattr(x, "__iter__"):
        return map(lambda y: extract_column(y), x)
    else:
        return extract_column(x)

""" It should implement the behaviour depicted in
http://docs.sqlalchemy.org/en/latest/orm/loading_relationships.html """


def testing_where_clauses_1(query_module):
    models_module = sqlalchemy_models
    query = query_module.get_query(models_module.Author).filter(models_module.Author.id == 1)

    rows = query.all()
    result = map(extract_row, rows)
    return str(result)


def testing_where_clauses_2(query_module):
    models_module = sqlalchemy_models
    query = query_module.get_query(models_module.Author).filter(models_module.Author.id != 1)

    rows = query.all()
    result = map(extract_row, rows)
    return str(result)


def testing_where_clauses_3(query_module):
    models_module = sqlalchemy_models
    query = query_module.get_query(models_module.Author).filter(models_module.Author.id > 1)

    rows = query.all()
    result = map(extract_row, rows)
    return str(result)


def testing_where_clauses_4(query_module):
    models_module = sqlalchemy_models
    query = query_module.get_query(models_module.Author).filter(models_module.Author.id < 2)

    rows = query.all()
    result = map(extract_row, rows)
    return str(result)


def testing_where_clauses_5(query_module):
    models_module = sqlalchemy_models
    query = query_module.get_query(models_module.Author).filter(models_module.Author.name == "Author1")

    rows = query.all()
    result = map(extract_row, rows)
    return str(result)


def testing_where_clauses_6(query_module):
    models_module = sqlalchemy_models
    query = query_module.get_query(models_module.Author).filter(models_module.Author.name != "Author1")

    rows = query.all()
    result = map(extract_row, rows)
    return str(result)


def compare(function, model_a, model_b):
    result_a = function(model_a)
    result_b = function(model_b)

    if result_a != result_b:
        logging.error("ERROR: %s != %s" % (result_a, result_b))
        return False
    else:
        return True

DATA_INITIALIZED = False


class TestWhereClauses(unittest.TestCase):

    def setUp(self):
        global DATA_INITIALIZED
        if not DATA_INITIALIZED:
            print("setUp")
            DATA_INITIALIZED = True
            init_mock_objects(sqlalchemy_models)
            init_mock_objects(rome_models)

    def test_where_clauses_compatibility_1(self):
        comparison_result = compare(testing_where_clauses_1, sqlalchemy_models, rome_models)
        self.assertEquals(comparison_result, True)

    def test_where_clauses_compatibility_2(self):
        comparison_result = compare(testing_where_clauses_2, sqlalchemy_models, rome_models)
        self.assertEquals(comparison_result, True)

    def test_where_clauses_compatibility_3(self):
        comparison_result = compare(testing_where_clauses_3, sqlalchemy_models, rome_models)
        self.assertEquals(comparison_result, True)

    def test_where_clauses_compatibility_4(self):
        comparison_result = compare(testing_where_clauses_4, sqlalchemy_models, rome_models)
        self.assertEquals(comparison_result, True)

    def test_where_clauses_compatibility_5(self):
        comparison_result = compare(testing_where_clauses_5, sqlalchemy_models, rome_models)
        self.assertEquals(comparison_result, True)

    def test_where_clauses_compatibility_6(self):
        comparison_result = compare(testing_where_clauses_5, sqlalchemy_models, rome_models)
        self.assertEquals(comparison_result, True)


# if __name__ == '__main__':
#     # Init objects
#     init_mock_objects(rome_models)
#     init_mock_objects(sqlalchemy_models)
#
#     # Basic join
#     compare(testing_where_clauses_1, sqlalchemy_models, rome_models)
#     compare(testing_where_clauses_2, sqlalchemy_models, rome_models)
#     compare(testing_where_clauses_3, sqlalchemy_models, rome_models)
#     compare(testing_where_clauses_4, sqlalchemy_models, rome_models)
#     compare(testing_where_clauses_5, sqlalchemy_models, rome_models)
#     compare(testing_where_clauses_6, sqlalchemy_models, rome_models)

