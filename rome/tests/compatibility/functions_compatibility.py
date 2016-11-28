import logging
import unittest

from sqlalchemy.sql.functions import count, sum, max, min

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


def testing_function_1(query_module):
    models_module = sqlalchemy_models
    query = query_module.get_query(count(models_module.Author.id))

    rows = query.all()
    result = map(extract_row, rows)
    return str(result)


def testing_function_2(query_module):
    models_module = sqlalchemy_models
    query = query_module.get_query(sum(models_module.Author.id))

    rows = query.all()
    result = map(extract_row, rows)
    return str(result)


def testing_function_3(query_module):
    models_module = sqlalchemy_models
    query = query_module.get_query(min(models_module.Author.id))

    rows = query.all()
    result = map(extract_row, rows)
    return str(result)


def testing_function_4(query_module):
    models_module = sqlalchemy_models
    query = query_module.get_query(max(models_module.Author.id))

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


class TestFunctions(unittest.TestCase):

    def setUp(self):
        global DATA_INITIALIZED
        if not DATA_INITIALIZED:
            print("setUp")
            DATA_INITIALIZED = True
            init_mock_objects(sqlalchemy_models)
            init_mock_objects(rome_models)

    def test_function_compatibility_1(self):
        comparison_result = compare(testing_function_1, sqlalchemy_models,
                                    rome_models)
        self.assertEquals(comparison_result, True)

    def test_function_compatibility_2(self):
        comparison_result = compare(testing_function_2, sqlalchemy_models,
                                    rome_models)
        self.assertEquals(comparison_result, True)

    def test_function_compatibility_3(self):
        comparison_result = compare(testing_function_3, sqlalchemy_models,
                                    rome_models)
        self.assertEquals(comparison_result, True)

    def test_function_compatibility_4(self):
        comparison_result = compare(testing_function_4, sqlalchemy_models,
                                    rome_models)
        self.assertEquals(comparison_result, True)


if __name__ == '__main__':
    unittest.main()
