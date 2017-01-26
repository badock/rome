import logging
import unittest

from models import *
from nova import exception
import api as db
from oslo_serialization import jsonutils
from nova.tests import uuidsentinel
from nova import test
from utils import clean_rome_all_databases
from nova.tests.unit import matchers

import copy
import context as rome_context


def init_mock_objects(models_module):
    models_module.drop_tables()
    models_module.create_tables()
    models_module.init_objects()


def _make_compute_node(host, node, hv_type, service_id):
    compute_node_dict = dict(vcpus=2, memory_mb=1024, local_gb=2048,
                        uuid=uuidsentinel.fake_compute_node,
                        vcpus_used=0, memory_mb_used=0,
                        local_gb_used=0, free_ram_mb=1024,
                        free_disk_gb=2048, hypervisor_type=hv_type,
                        hypervisor_version=1, cpu_info="",
                        running_vms=0, current_workload=0,
                        service_id=service_id,
                        host=host,
                        disk_available_least=100,
                        hypervisor_hostname=node,
                        host_ip='127.0.0.1',
                        supported_instances='',
                        pci_stats='',
                        metrics='',
                        extra_resources='',
                        cpu_allocation_ratio=16.0,
                        ram_allocation_ratio=1.5,
                        disk_allocation_ratio=1.0,
                        stats='', numa_topology='')
    # add some random stats
    stats = dict(num_instances=3, num_proj_12345=2,
            num_proj_23456=2, num_vm_building=3)
    compute_node_dict['stats'] = jsonutils.dumps(stats)
    return compute_node_dict


class ModelsObjectComparatorMixin(object):
    def _dict_from_object(self, obj, ignored_keys):
        if ignored_keys is None:
            ignored_keys = []

        return {k: v for k, v in obj.items()
                if k not in ignored_keys}

    def _assertEqualObjects(self, obj1, obj2, ignored_keys=None):
        obj1 = self._dict_from_object(obj1, ignored_keys)
        obj2 = self._dict_from_object(obj2, ignored_keys)

        self.assertEqual(len(obj1),
                         len(obj2),
                         "Keys mismatch: %s" %
                          str(set(obj1.keys()) ^ set(obj2.keys())))
        for key, value in obj1.items():
            self.assertEqual(value, obj2[key])

    def _assertEqualListsOfObjects(self, objs1, objs2, ignored_keys=None):
        obj_to_dict = lambda o: self._dict_from_object(o, ignored_keys)
        sort_key = lambda d: [d[k] for k in sorted(d)]
        conv_and_sort = lambda obj: sorted(map(obj_to_dict, obj), key=sort_key)

        self.assertEqual(conv_and_sort(objs1), conv_and_sort(objs2))

    def _assertEqualOrderedListOfObjects(self, objs1, objs2,
                                         ignored_keys=None):
        obj_to_dict = lambda o: self._dict_from_object(o, ignored_keys)
        conv = lambda objs: [obj_to_dict(obj) for obj in objs]

        self.assertEqual(conv(objs1), conv(objs2))

    def _assertEqualListsOfPrimitivesAsSets(self, primitives1, primitives2):
        self.assertEqual(len(primitives1), len(primitives2))
        for primitive in primitives1:
            self.assertIn(primitive, primitives2)

        for primitive in primitives2:
            self.assertIn(primitive, primitives1)


def _get_fake_aggr_values():
    return {'name': 'fake_aggregate'}


def _get_fake_aggr_metadata():
    return {'fake_key1': 'fake_value1',
            'fake_key2': 'fake_value2',
            'availability_zone': 'fake_avail_zone'}


def _get_fake_aggr_hosts():
    return ['foo.openstack.org']


def _create_aggregate(context=rome_context.get_admin_context(),
                      values=_get_fake_aggr_values(),
                      metadata=_get_fake_aggr_metadata()):
    return db.aggregate_create(context, values, metadata)


def _create_aggregate_with_hosts(context=rome_context.get_admin_context(),
                      values=_get_fake_aggr_values(),
                      metadata=_get_fake_aggr_metadata(),
                      hosts=_get_fake_aggr_hosts()):
    result = _create_aggregate(context=context,
                               values=values, metadata=metadata)
    for host in hosts:
        db.aggregate_host_add(context, result['id'], host)
    return result


class AggregateDBApiTestCase(test.TestCase, ModelsObjectComparatorMixin):
    def setUp(self):
        from nova import context
        super(AggregateDBApiTestCase, self).setUp()
        self.user_id = 'fake'
        self.project_id = 'fake'
        # self.context = context.RequestContext(self.user_id, self.project_id)
        clean_rome_all_databases()

    def assertThat(self, matchee, matcher, message='', verbose=False):
        """Assert that matchee is matched by matcher.

        :param matchee: An object to match with matcher.
        :param matcher: An object meeting the testtools.Matcher protocol.
        :raises MismatchError: When matcher does not match thing.
        """
        mismatch_error = self._matchHelper(matchee, matcher, message, verbose)
        if mismatch_error is not None:
            raise mismatch_error

    def test_aggregate_create_no_metadata(self):
        result = _create_aggregate(metadata=None)
        self.assertEqual(result['name'], 'fake_aggregate')

    # def test_aggregate_create_avoid_name_conflict(self):
    #     r1 = _create_aggregate(metadata=None)
    #     db.aggregate_delete(rome_context.get_admin_context(), r1['id'])
    #     values = {'name': r1['name']}
    #     metadata = {'availability_zone': 'new_zone'}
    #     r2 = _create_aggregate(values=values, metadata=metadata)
    #     self.assertEqual(r2['name'], values['name'])
    #     self.assertEqual(r2['availability_zone'],
    #             metadata['availability_zone'])

    def test_aggregate_create_raise_exist_exc(self):
        _create_aggregate(metadata=None)
        self.assertRaises(exception.AggregateNameExists,
                          _create_aggregate, metadata=None)

    def test_aggregate_get_raise_not_found(self):
        ctxt = rome_context.get_admin_context()
        # this does not exist!
        aggregate_id = 1
        self.assertRaises(exception.AggregateNotFound,
                          db.aggregate_get,
                          ctxt, aggregate_id)

    def test_aggregate_get_by_uuid_raise_not_found(self):
        ctxt = rome_context.get_admin_context()
        aggregate_uuid = uuidsentinel.missing_aggregate_uuid
        self.assertRaises(exception.AggregateNotFound,
                          db.aggregate_get_by_uuid,
                          ctxt, aggregate_uuid)

    def test_aggregate_metadata_get_raise_not_found(self):
        ctxt = rome_context.get_admin_context()
        # this does not exist!
        aggregate_id = 1
        self.assertRaises(exception.AggregateNotFound,
                          db.aggregate_metadata_get,
                          ctxt, aggregate_id)

    def test_aggregate_create_with_metadata(self):
        ctxt = rome_context.get_admin_context()
        result = _create_aggregate(context=ctxt)
        expected_metadata = db.aggregate_metadata_get(ctxt, result['id'])
        self.assertThat(expected_metadata,
                        matchers.DictMatches(_get_fake_aggr_metadata()))

    def test_aggregate_create_delete_create_with_metadata(self):
        # test for bug 1052479
        ctxt = rome_context.get_admin_context()
        result = _create_aggregate(context=ctxt)
        expected_metadata = db.aggregate_metadata_get(ctxt, result['id'])
        self.assertThat(expected_metadata,
                        matchers.DictMatches(_get_fake_aggr_metadata()))
        db.aggregate_delete(ctxt, result['id'])
        result = _create_aggregate(metadata={'availability_zone':
            'fake_avail_zone'})
        expected_metadata = db.aggregate_metadata_get(ctxt, result['id'])
        self.assertEqual(expected_metadata, {'availability_zone':
            'fake_avail_zone'})

    # def test_aggregate_get(self):
    #     ctxt = rome_context.get_admin_context()
    #     result = _create_aggregate_with_hosts(context=ctxt)
    #     expected = db.aggregate_get(ctxt, result['id'])
    #     self.assertEqual(_get_fake_aggr_hosts(), expected['hosts'])
    #     self.assertEqual(_get_fake_aggr_metadata(), expected['metadetails'])
    #
    # def test_aggregate_get_by_uuid(self):
    #     ctxt = rome_context.get_admin_context()
    #     result = _create_aggregate_with_hosts(context=ctxt)
    #     expected = db.aggregate_get_by_uuid(ctxt, result['uuid'])
    #     self.assertEqual(_get_fake_aggr_hosts(), expected['hosts'])
    #     self.assertEqual(_get_fake_aggr_metadata(), expected['metadetails'])

    def test_aggregate_get_by_host(self):
        ctxt = rome_context.get_admin_context()
        values2 = {'name': 'fake_aggregate2'}
        values3 = {'name': 'fake_aggregate3'}
        values4 = {'name': 'fake_aggregate4'}
        values5 = {'name': 'fake_aggregate5'}
        a1 = _create_aggregate_with_hosts(context=ctxt)
        a2 = _create_aggregate_with_hosts(context=ctxt, values=values2)
        # a3 has no hosts and should not be in the results.
        _create_aggregate(context=ctxt, values=values3)
        # a4 has no matching hosts.
        _create_aggregate_with_hosts(context=ctxt, values=values4,
                hosts=['foo4.openstack.org'])
        # a5 has no matching hosts after deleting the only matching host.
        a5 = _create_aggregate_with_hosts(context=ctxt, values=values5,
                hosts=['foo5.openstack.org', 'foo.openstack.org'])
        db.aggregate_host_delete(ctxt, a5['id'],
                                 'foo.openstack.org')
        r1 = db.aggregate_get_by_host(ctxt, 'foo.openstack.org')
        self.assertEqual([a1['id'], a2['id']], [x['id'] for x in r1])

    def test_aggregate_get_by_host_with_key(self):
        ctxt = rome_context.get_admin_context()
        values2 = {'name': 'fake_aggregate2'}
        values3 = {'name': 'fake_aggregate3'}
        values4 = {'name': 'fake_aggregate4'}
        a1 = _create_aggregate_with_hosts(context=ctxt,
                                          metadata={'goodkey': 'good'})
        _create_aggregate_with_hosts(context=ctxt, values=values2)
        _create_aggregate(context=ctxt, values=values3)
        _create_aggregate_with_hosts(context=ctxt, values=values4,
                hosts=['foo4.openstack.org'], metadata={'goodkey': 'bad'})
        # filter result by key
        r1 = db.aggregate_get_by_host(ctxt, 'foo.openstack.org', key='goodkey')
        self.assertEqual([a1['id']], [x['id'] for x in r1])

    # def test_aggregate_metadata_get_by_host(self):
    #     ctxt = rome_context.get_admin_context()
    #     values = {'name': 'fake_aggregate2'}
    #     values2 = {'name': 'fake_aggregate3'}
    #     _create_aggregate_with_hosts(context=ctxt)
    #     _create_aggregate_with_hosts(context=ctxt, values=values)
    #     _create_aggregate_with_hosts(context=ctxt, values=values2,
    #             hosts=['bar.openstack.org'], metadata={'badkey': 'bad'})
    #     r1 = db.aggregate_metadata_get_by_host(ctxt, 'foo.openstack.org')
    #     self.assertEqual(r1['fake_key1'], set(['fake_value1']))
    #     self.assertNotIn('badkey', r1)

    # def test_aggregate_metadata_get_by_host_with_key(self):
    #     ctxt = rome_context.get_admin_context()
    #     values2 = {'name': 'fake_aggregate12'}
    #     values3 = {'name': 'fake_aggregate23'}
    #     a2_hosts = ['foo1.openstack.org', 'foo2.openstack.org']
    #     a2_metadata = {'good': 'value12', 'bad': 'badvalue12'}
    #     a3_hosts = ['foo2.openstack.org', 'foo3.openstack.org']
    #     a3_metadata = {'good': 'value23', 'bad': 'badvalue23'}
    #     _create_aggregate_with_hosts(context=ctxt)
    #     _create_aggregate_with_hosts(context=ctxt, values=values2,
    #             hosts=a2_hosts, metadata=a2_metadata)
    #     a3 = _create_aggregate_with_hosts(context=ctxt, values=values3,
    #             hosts=a3_hosts, metadata=a3_metadata)
    #     r1 = db.aggregate_metadata_get_by_host(ctxt, 'foo2.openstack.org',
    #                                            key='good')
    #     self.assertEqual(r1['good'], set(['value12', 'value23']))
    #     self.assertNotIn('fake_key1', r1)
    #     self.assertNotIn('bad', r1)
    #     # Delete metadata
    #     db.aggregate_metadata_delete(ctxt, a3['id'], 'good')
    #     r2 = db.aggregate_metadata_get_by_host(ctxt, 'foo3.openstack.org',
    #                                            key='good')
    #     self.assertNotIn('good', r2)

    def test_aggregate_get_by_host_not_found(self):
        ctxt = rome_context.get_admin_context()
        _create_aggregate_with_hosts(context=ctxt)
        self.assertEqual([], db.aggregate_get_by_host(ctxt, 'unknown_host'))

    # def test_aggregate_delete_raise_not_found(self):
    #     ctxt = rome_context.get_admin_context()
    #     # this does not exist!
    #     aggregate_id = 1
    #     self.assertRaises(exception.AggregateNotFound,
    #                       db.aggregate_delete,
    #                       ctxt, aggregate_id)

    # def test_aggregate_delete(self):
    #     ctxt = rome_context.get_admin_context()
    #     result = _create_aggregate(context=ctxt, metadata=None)
    #     db.aggregate_delete(ctxt, result['id'])
    #     expected = db.aggregate_get_all(ctxt)
    #     self.assertEqual(0, len(expected))
    #     aggregate = db.aggregate_get(ctxt.elevated(read_deleted='yes'),
    #                                  result['id'])
    #     self.assertEqual(aggregate['deleted'], result['id'])

    # def test_aggregate_update(self):
    #     ctxt = rome_context.get_admin_context()
    #     result = _create_aggregate(context=ctxt, metadata={'availability_zone':
    #         'fake_avail_zone'})
    #     self.assertEqual(result['availability_zone'], 'fake_avail_zone')
    #     new_values = _get_fake_aggr_values()
    #     new_values['availability_zone'] = 'different_avail_zone'
    #     updated = db.aggregate_update(ctxt, result['id'], new_values)
    #     self.assertNotEqual(result['availability_zone'],
    #                         updated['availability_zone'])

    # def test_aggregate_update_with_metadata(self):
    #     ctxt = rome_context.get_admin_context()
    #     result = _create_aggregate(context=ctxt, metadata=None)
    #     values = _get_fake_aggr_values()
    #     values['metadata'] = _get_fake_aggr_metadata()
    #     values['availability_zone'] = 'different_avail_zone'
    #     expected_metadata = copy.deepcopy(values['metadata'])
    #     expected_metadata['availability_zone'] = values['availability_zone']
    #     db.aggregate_update(ctxt, result['id'], values)
    #     metadata = db.aggregate_metadata_get(ctxt, result['id'])
    #     updated = db.aggregate_get(ctxt, result['id'])
    #     self.assertThat(metadata,
    #                     matchers.DictMatches(expected_metadata))
    #     self.assertNotEqual(result['availability_zone'],
    #                         updated['availability_zone'])

    def test_aggregate_update_with_existing_metadata(self):
        ctxt = rome_context.get_admin_context()
        result = _create_aggregate(context=ctxt)
        values = _get_fake_aggr_values()
        values['metadata'] = _get_fake_aggr_metadata()
        values['metadata']['fake_key1'] = 'foo'
        expected_metadata = copy.deepcopy(values['metadata'])
        db.aggregate_update(ctxt, result['id'], values)
        metadata = db.aggregate_metadata_get(ctxt, result['id'])
        self.assertThat(metadata, matchers.DictMatches(expected_metadata))

    def test_aggregate_update_zone_with_existing_metadata(self):
        ctxt = rome_context.get_admin_context()
        result = _create_aggregate(context=ctxt)
        new_zone = {'availability_zone': 'fake_avail_zone_2'}
        metadata = _get_fake_aggr_metadata()
        metadata.update(new_zone)
        db.aggregate_update(ctxt, result['id'], new_zone)
        expected = db.aggregate_metadata_get(ctxt, result['id'])
        self.assertThat(metadata, matchers.DictMatches(expected))

    def test_aggregate_update_raise_not_found(self):
        ctxt = rome_context.get_admin_context()
        # this does not exist!
        aggregate_id = 1
        new_values = _get_fake_aggr_values()
        self.assertRaises(exception.AggregateNotFound,
                          db.aggregate_update, ctxt, aggregate_id, new_values)

    def test_aggregate_update_raise_name_exist(self):
        ctxt = rome_context.get_admin_context()
        _create_aggregate(context=ctxt, values={'name': 'test1'},
                          metadata={'availability_zone': 'fake_avail_zone'})
        _create_aggregate(context=ctxt, values={'name': 'test2'},
                          metadata={'availability_zone': 'fake_avail_zone'})
        aggregate_id = 1
        new_values = {'name': 'test2'}
        self.assertRaises(exception.AggregateNameExists,
                          db.aggregate_update, ctxt, aggregate_id, new_values)

    def test_aggregate_get_all(self):
        ctxt = rome_context.get_admin_context()
        counter = 3
        for c in range(counter):
            _create_aggregate(context=ctxt,
                              values={'name': 'fake_aggregate_%d' % c},
                              metadata=None)
        results = db.aggregate_get_all(ctxt)
        self.assertEqual(len(results), counter)

    def test_aggregate_get_all_non_deleted(self):
        ctxt = rome_context.get_admin_context()
        add_counter = 5
        remove_counter = 2
        aggregates = []
        for c in range(1, add_counter):
            values = {'name': 'fake_aggregate_%d' % c}
            aggregates.append(_create_aggregate(context=ctxt,
                                                values=values, metadata=None))
        for c in range(1, remove_counter):
            db.aggregate_delete(ctxt, aggregates[c - 1]['id'])
        results = db.aggregate_get_all(ctxt)
        self.assertEqual(len(results), add_counter - remove_counter)

    def test_aggregate_metadata_add(self):
        ctxt = rome_context.get_admin_context()
        result = _create_aggregate(context=ctxt, metadata=None)
        metadata = _get_fake_aggr_metadata()
        db.aggregate_metadata_add(ctxt, result['id'], metadata)
        expected = db.aggregate_metadata_get(ctxt, result['id'])
        self.assertThat(metadata, matchers.DictMatches(expected))

    def test_aggregate_metadata_add_empty_metadata(self):
        ctxt = rome_context.get_admin_context()
        result = _create_aggregate(context=ctxt, metadata=None)
        metadata = {}
        db.aggregate_metadata_add(ctxt, result['id'], metadata)
        expected = db.aggregate_metadata_get(ctxt, result['id'])
        self.assertThat(metadata, matchers.DictMatches(expected))

    def test_aggregate_metadata_add_and_update(self):
        ctxt = rome_context.get_admin_context()
        result = _create_aggregate(context=ctxt)
        metadata = _get_fake_aggr_metadata()
        key = list(metadata.keys())[0]
        new_metadata = {key: 'foo',
                        'fake_new_key': 'fake_new_value'}
        metadata.update(new_metadata)
        db.aggregate_metadata_add(ctxt, result['id'], new_metadata)
        expected = db.aggregate_metadata_get(ctxt, result['id'])
        self.assertThat(metadata, matchers.DictMatches(expected))

    def test_aggregate_metadata_add_retry(self):
        from nova.db.sqlalchemy import api as sqlalchemy_api
        from oslo_db import exception as db_exc

        ctxt = rome_context.get_admin_context()
        result = _create_aggregate(context=ctxt, metadata=None)

        def counted():
            def get_query(context, id, read_deleted):
                get_query.counter += 1
                raise db_exc.DBDuplicateEntry
            get_query.counter = 0
            return get_query

        get_query = counted()
        self.stubs.Set(sqlalchemy_api,
                       '_aggregate_metadata_get_query', get_query)
        self.assertRaises(db_exc.DBDuplicateEntry, sqlalchemy_api.
                          aggregate_metadata_add, ctxt, result['id'], {},
                          max_retries=5)
        self.assertEqual(get_query.counter, 5)

    def test_aggregate_metadata_update(self):
        ctxt = rome_context.get_admin_context()
        result = _create_aggregate(context=ctxt)
        metadata = _get_fake_aggr_metadata()
        key = list(metadata.keys())[0]
        db.aggregate_metadata_delete(ctxt, result['id'], key)
        new_metadata = {key: 'foo'}
        db.aggregate_metadata_add(ctxt, result['id'], new_metadata)
        expected = db.aggregate_metadata_get(ctxt, result['id'])
        metadata[key] = 'foo'
        self.assertThat(metadata, matchers.DictMatches(expected))

    def test_aggregate_metadata_delete(self):
        ctxt = rome_context.get_admin_context()
        result = _create_aggregate(context=ctxt, metadata=None)
        metadata = _get_fake_aggr_metadata()
        db.aggregate_metadata_add(ctxt, result['id'], metadata)
        db.aggregate_metadata_delete(ctxt, result['id'],
                                     list(metadata.keys())[0])
        expected = db.aggregate_metadata_get(ctxt, result['id'])
        del metadata[list(metadata.keys())[0]]
        self.assertThat(metadata, matchers.DictMatches(expected))

    def test_aggregate_remove_availability_zone(self):
        ctxt = rome_context.get_admin_context()
        result = _create_aggregate(context=ctxt, metadata={'availability_zone':
            'fake_avail_zone'})
        db.aggregate_metadata_delete(ctxt, result['id'], 'availability_zone')
        expected = db.aggregate_metadata_get(ctxt, result['id'])
        aggregate = db.aggregate_get(ctxt, result['id'])
        self.assertIsNone(aggregate['availability_zone'])
        self.assertThat({}, matchers.DictMatches(expected))

    def test_aggregate_metadata_delete_raise_not_found(self):
        ctxt = rome_context.get_admin_context()
        result = _create_aggregate(context=ctxt)
        self.assertRaises(exception.AggregateMetadataNotFound,
                          db.aggregate_metadata_delete,
                          ctxt, result['id'], 'foo_key')

    def test_aggregate_host_add(self):
        ctxt = rome_context.get_admin_context()
        result = _create_aggregate_with_hosts(context=ctxt, metadata=None)
        expected = db.aggregate_host_get_all(ctxt, result['id'])
        self.assertEqual(_get_fake_aggr_hosts(), expected)

    def test_aggregate_host_re_add(self):
        ctxt = rome_context.get_admin_context()
        result = _create_aggregate_with_hosts(context=ctxt, metadata=None)
        host = _get_fake_aggr_hosts()[0]
        db.aggregate_host_delete(ctxt, result['id'], host)
        db.aggregate_host_add(ctxt, result['id'], host)
        expected = db.aggregate_host_get_all(ctxt, result['id'])
        self.assertEqual(len(expected), 1)

    def test_aggregate_host_add_duplicate_works(self):
        ctxt = rome_context.get_admin_context()
        r1 = _create_aggregate_with_hosts(context=ctxt, metadata=None)
        r2 = _create_aggregate_with_hosts(ctxt,
                          values={'name': 'fake_aggregate2'},
                          metadata={'availability_zone': 'fake_avail_zone2'})
        h1 = db.aggregate_host_get_all(ctxt, r1['id'])
        h2 = db.aggregate_host_get_all(ctxt, r2['id'])
        self.assertEqual(h1, h2)

    # def test_aggregate_host_add_duplicate_raise_exist_exc(self):
    #     ctxt = rome_context.get_admin_context()
    #     result = _create_aggregate_with_hosts(context=ctxt, metadata=None)
    #     self.assertRaises(exception.AggregateHostExists,
    #                       db.aggregate_host_add,
    #                       ctxt, result['id'], _get_fake_aggr_hosts()[0])

    def test_aggregate_host_add_raise_not_found(self):
        ctxt = rome_context.get_admin_context()
        # this does not exist!
        aggregate_id = 1
        host = _get_fake_aggr_hosts()[0]
        self.assertRaises(exception.AggregateNotFound,
                          db.aggregate_host_add,
                          ctxt, aggregate_id, host)

    def test_aggregate_host_delete(self):
        ctxt = rome_context.get_admin_context()
        result = _create_aggregate_with_hosts(context=ctxt, metadata=None)
        db.aggregate_host_delete(ctxt, result['id'],
                                 _get_fake_aggr_hosts()[0])
        expected = db.aggregate_host_get_all(ctxt, result['id'])
        self.assertEqual(0, len(expected))

    def test_aggregate_host_delete_raise_not_found(self):
        ctxt = rome_context.get_admin_context()
        result = _create_aggregate(context=ctxt)
        self.assertRaises(exception.AggregateHostNotFound,
                          db.aggregate_host_delete,
                          ctxt, result['id'], _get_fake_aggr_hosts()[0])

if __name__ == '__main__':
    unittest.main()
