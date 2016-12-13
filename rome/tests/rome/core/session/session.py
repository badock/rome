import logging
import unittest

import sqlalchemy_models
import threading

import rome_models
from rome.core.session.utils import retry_on_db_deadlock
from rome.core.session.session import DBDeadlock
from sqlalchemy_models import NB_ADDRESS


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


def network_address_allocate(network_id, models_module):

    try:
        session = models_module.get_session()
        first_available_address = session.query(models_module.NetworkAddress).filter(
            models_module.NetworkAddress.allocated == False).first()

        session.add(first_available_address)

        first_available_address.allocated = True
        first_available_address.network_id = network_id

        session.commit()
    except DBDeadlock:
        logging.info("A session (%s) could not commit")


@retry_on_db_deadlock
def network_address_allocate_with_retry(network_id, models_module):

    session = models_module.get_session()

    with session.begin():
        first_available_address = session.query(models_module.NetworkAddress).filter(
            models_module.NetworkAddress.allocated == False).first()

        session.add(first_available_address)

        first_available_address.allocated = True
        first_available_address.network_id = network_id


def test_sessions_1(query_module):
    models_module = sqlalchemy_models

    threads = []
    for i in range(0, NB_ADDRESS):
        t = threading.Thread(target=network_address_allocate,
                             args=(i, query_module))
        threads += [t]

    for t in threads:
        t.start()

    for t in threads:
        t.join()

    allocated_address_count = query_module.get_query(models_module.NetworkAddress).filter(
        models_module.NetworkAddress.allocated == True).count()
    unallocated_address_count = query_module.get_query(models_module.NetworkAddress).filter(
        models_module.NetworkAddress.allocated == False).count()

    for address in query_module.get_query(models_module.NetworkAddress).all():
        print("%s => %s,%s" % (address.id, address.allocated, address.network_id))

    return {
        "allocated_address_count": allocated_address_count,
        "unallocated_address_count": unallocated_address_count
    }


def test_sessions_2(query_module):
    models_module = sqlalchemy_models

    threads = []
    for i in range(0, NB_ADDRESS):
        t = threading.Thread(target=network_address_allocate_with_retry,
                             args=(i, query_module))
        threads += [t]

    for t in threads:
        t.start()

    for t in threads:
        t.join()

    allocated_address_count = query_module.get_query(models_module.NetworkAddress).filter(
        models_module.NetworkAddress.allocated == True).count()
    unallocated_address_count = query_module.get_query(models_module.NetworkAddress).filter(
        models_module.NetworkAddress.allocated == False).count()

    for address in query_module.get_query(models_module.NetworkAddress).all():
        print("%s => %s,%s" % (address.id, address.allocated, address.network_id))

    return {
        "allocated_address_count": allocated_address_count,
        "unallocated_address_count": unallocated_address_count
    }


def compare(function, model_a, model_b):
    result_a = function(model_a)
    result_b = function(model_b)

    if result_a != result_b:
        logging.error("ERROR: %s != %s" % (result_a, result_b))
        return False
    else:
        return True


DATA_INITIALIZED = False
NB_RUN = 5


class TestSessions(unittest.TestCase):

    def setUp(self):
        init_mock_objects(sqlalchemy_models)
        init_mock_objects(rome_models)

    # def test_sessions_1(self):
    #     result = test_sessions_1(rome_models)
    #     self.assertNotEqual(result["allocated_address_count"], NB_ADDRESS)
    #     self.assertNotEqual(result["unallocated_address_count"], 0)

    def test_sessions_2(self):
        for i in range(0, NB_RUN):
            self.setUp()
            # logging.info("running test_sessions_2 %s/%s" % (i, NB_RUN))
            result = test_sessions_2(rome_models)
            self.assertEqual(result["allocated_address_count"], NB_ADDRESS)
            self.assertEqual(result["unallocated_address_count"], 0)

if __name__ == '__main__':
    unittest.main()
