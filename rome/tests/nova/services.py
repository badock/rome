import logging
import unittest

from models import *
from nova import exception
import api as db
from oslo_serialization import jsonutils
from nova.tests import uuidsentinel
from nova import test

import context as rome_context
from utils import clean_rome_all_databases


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


class ServiceTestCase(unittest.TestCase, ModelsObjectComparatorMixin):
    def setUp(self):
        super(ServiceTestCase, self).setUp()
        self.ctxt = rome_context.get_admin_context()
        clean_rome_all_databases()

    def _get_base_values(self):
        return {
            'host': 'fake_host',
            'binary': 'fake_binary',
            'topic': 'fake_topic',
            'report_count': 3,
            'disabled': False,
            'forced_down': False
        }

    def _create_service(self, values):
        v = self._get_base_values()
        v.update(values)
        return db.service_create(self.ctxt, v)

    def test_service_create(self):
        service = self._create_service({})
        self.assertIsNotNone(service['id'])
        for key, value in self._get_base_values().items():
            self.assertEqual(value, service[key])

    # def test_service_create_disabled(self):
    #     self.flags(enable_new_services=False)
    #     service = self._create_service({})
    #     self.assertTrue(service['disabled'])

    # def test_service_create_disabled_reason(self):
    #     self.flags(enable_new_services=False)
    #     service = self._create_service({})
    #     msg = "New service disabled due to config option."
    #     self.assertEqual(msg, service['disabled_reason'])

    def test_service_destroy(self):
        service1 = self._create_service({})
        service2 = self._create_service({'host': 'fake_host2'})

        db.service_destroy(self.ctxt, service1['id'])
        self.assertRaises(exception.ServiceNotFound,
                          db.service_get, self.ctxt, service1['id'])
        self._assertEqualObjects(db.service_get(self.ctxt, service2['id']),
                                 service2, ignored_keys=['compute_node'])

    def test_service_update(self):
        service = self._create_service({})
        new_values = {
            'host': 'fake_host1',
            'binary': 'fake_binary1',
            'topic': 'fake_topic1',
            'report_count': 4,
            'disabled': True
        }
        db.service_update(self.ctxt, service['id'], new_values)
        updated_service = db.service_get(self.ctxt, service['id'])
        for key, value in new_values.items():
            self.assertEqual(value, updated_service[key])

    def test_service_update_not_found_exception(self):
        self.assertRaises(exception.ServiceNotFound,
                          db.service_update, self.ctxt, 100500, {})

    def test_service_update_with_set_forced_down(self):
        service = self._create_service({})
        db.service_update(self.ctxt, service['id'], {'forced_down': True})
        updated_service = db.service_get(self.ctxt, service['id'])
        self.assertTrue(updated_service['forced_down'])

    def test_service_update_with_unset_forced_down(self):
        service = self._create_service({'forced_down': True})
        db.service_update(self.ctxt, service['id'], {'forced_down': False})
        updated_service = db.service_get(self.ctxt, service['id'])
        self.assertFalse(updated_service['forced_down'])

    def test_service_get(self):
        service1 = self._create_service({})
        self._create_service({'host': 'some_other_fake_host'})
        real_service1 = db.service_get(self.ctxt, service1['id'])
        self._assertEqualObjects(service1, real_service1,
                                 ignored_keys=['compute_node'])

    # def test_service_get_minimum_version(self):
    #     self._create_service({'version': 1,
    #                           'host': 'host3',
    #                           'binary': 'compute',
    #                           'forced_down': True})
    #     self._create_service({'version': 2,
    #                           'host': 'host1',
    #                           'binary': 'compute'})
    #     self._create_service({'version': 3,
    #                           'host': 'host2',
    #                           'binary': 'compute'})
    #     self.assertEqual({'compute': 2},
    #                      db.service_get_minimum_version(self.ctxt,
    #                                                     ['compute']))

    def test_service_get_not_found_exception(self):
        self.assertRaises(exception.ServiceNotFound,
                          db.service_get, self.ctxt, 100500)

    def test_service_get_by_host_and_topic(self):
        service1 = self._create_service({'host': 'host1', 'topic': 'topic1'})
        self._create_service({'host': 'host2', 'topic': 'topic2'})

        real_service1 = db.service_get_by_host_and_topic(self.ctxt,
                                                         host='host1',
                                                         topic='topic1')
        self._assertEqualObjects(service1, real_service1)

    def test_service_get_by_host_and_binary(self):
        service1 = self._create_service({'host': 'host1', 'binary': 'foo'})
        self._create_service({'host': 'host2', 'binary': 'bar'})

        real_service1 = db.service_get_by_host_and_binary(self.ctxt,
                                                         host='host1',
                                                         binary='foo')
        self._assertEqualObjects(service1, real_service1)

    def test_service_get_by_host_and_binary_raises(self):
        self.assertRaises(exception.HostBinaryNotFound,
                          db.service_get_by_host_and_binary, self.ctxt,
                          host='host1', binary='baz')

    def test_service_get_all(self):
        values = [
            {'host': 'host1', 'topic': 'topic1'},
            {'host': 'host2', 'topic': 'topic2'},
            {'disabled': True}
        ]
        services = [self._create_service(vals) for vals in values]
        disabled_services = [services[-1]]
        non_disabled_services = services[:-1]

        compares = [
            (services, db.service_get_all(self.ctxt)),
            (disabled_services, db.service_get_all(self.ctxt, True)),
            (non_disabled_services, db.service_get_all(self.ctxt, False))
        ]
        for comp in compares:
            self._assertEqualListsOfObjects(*comp)

    def test_service_get_all_by_topic(self):
        values = [
            {'host': 'host1', 'topic': 't1'},
            {'host': 'host2', 'topic': 't1'},
            {'disabled': True, 'topic': 't1'},
            {'host': 'host3', 'topic': 't2'}
        ]
        services = [self._create_service(vals) for vals in values]
        expected = services[:2]
        real = db.service_get_all_by_topic(self.ctxt, 't1')
        self._assertEqualListsOfObjects(expected, real)

    def test_service_get_all_by_binary(self):
        values = [
            {'host': 'host1', 'binary': 'b1'},
            {'host': 'host2', 'binary': 'b1'},
            {'disabled': True, 'binary': 'b1'},
            {'host': 'host3', 'binary': 'b2'}
        ]
        services = [self._create_service(vals) for vals in values]
        expected = services[:2]
        real = db.service_get_all_by_binary(self.ctxt, 'b1')
        self._assertEqualListsOfObjects(expected, real)

    def test_service_get_all_by_binary_include_disabled(self):
        values = [
            {'host': 'host1', 'binary': 'b1'},
            {'host': 'host2', 'binary': 'b1'},
            {'disabled': True, 'binary': 'b1'},
            {'host': 'host3', 'binary': 'b2'}
        ]
        services = [self._create_service(vals) for vals in values]
        expected = services[:3]
        real = db.service_get_all_by_binary(self.ctxt, 'b1',
                                            include_disabled=True)
        self._assertEqualListsOfObjects(expected, real)

    def test_service_get_all_computes_by_hv_type(self):
        values = [
            {'host': 'host1', 'binary': 'nova-compute'},
            {'host': 'host2', 'binary': 'nova-compute', 'disabled': True},
            {'host': 'host3', 'binary': 'nova-compute'},
            {'host': 'host4', 'binary': 'b2'}
        ]
        services = [self._create_service(vals) for vals in values]
        compute_nodes = [
            _make_compute_node('host1', 'node1', 'ironic', services[0]['id']),
            _make_compute_node('host1', 'node2', 'ironic', services[0]['id']),
            _make_compute_node('host2', 'node3', 'ironic', services[1]['id']),
            _make_compute_node('host3', 'host3', 'kvm', services[2]['id']),
        ]
        [db.compute_node_create(self.ctxt, cn) for cn in compute_nodes]

        expected = services[:1]
        real = db.service_get_all_computes_by_hv_type(self.ctxt,
                                                      'ironic',
                                                      include_disabled=False)
        self._assertEqualListsOfObjects(expected, real)

    def test_service_get_all_computes_by_hv_type_include_disabled(self):
        values = [
            {'host': 'host1', 'binary': 'nova-compute'},
            {'host': 'host2', 'binary': 'nova-compute', 'disabled': True},
            {'host': 'host3', 'binary': 'nova-compute'},
            {'host': 'host4', 'binary': 'b2'}
        ]
        services = [self._create_service(vals) for vals in values]
        compute_nodes = [
            _make_compute_node('host1', 'node1', 'ironic', services[0]['id']),
            _make_compute_node('host1', 'node2', 'ironic', services[0]['id']),
            _make_compute_node('host2', 'node3', 'ironic', services[1]['id']),
            _make_compute_node('host3', 'host3', 'kvm', services[2]['id']),
        ]
        [db.compute_node_create(self.ctxt, cn) for cn in compute_nodes]

        expected = services[:2]
        real = db.service_get_all_computes_by_hv_type(self.ctxt,
                                                      'ironic',
                                                      include_disabled=True)
        self._assertEqualListsOfObjects(expected, real)

    def test_service_get_all_by_host(self):
        values = [
            {'host': 'host1', 'topic': 't11', 'binary': 'b11'},
            {'host': 'host1', 'topic': 't12', 'binary': 'b12'},
            {'host': 'host2', 'topic': 't1'},
            {'host': 'host3', 'topic': 't1'}
        ]
        services = [self._create_service(vals) for vals in values]

        expected = services[:2]
        real = db.service_get_all_by_host(self.ctxt, 'host1')
        self._assertEqualListsOfObjects(expected, real)

    def test_service_get_by_compute_host(self):
        values = [
            {'host': 'host1', 'binary': 'nova-compute'},
            {'host': 'host2', 'binary': 'nova-scheduler'},
            {'host': 'host3', 'binary': 'nova-compute'}
        ]
        services = [self._create_service(vals) for vals in values]

        real_service = db.service_get_by_compute_host(self.ctxt, 'host1')
        self._assertEqualObjects(services[0], real_service)

        self.assertRaises(exception.ComputeHostNotFound,
                          db.service_get_by_compute_host,
                          self.ctxt, 'non-exists-host')

    def test_service_get_by_compute_host_not_found(self):
        self.assertRaises(exception.ComputeHostNotFound,
                          db.service_get_by_compute_host,
                          self.ctxt, 'non-exists-host')

    # def test_service_binary_exists_exception(self):
    #     db.service_create(self.ctxt, self._get_base_values())
    #     values = self._get_base_values()
    #     values.update({'topic': 'top1'})
    #     self.assertRaises(exception.ServiceBinaryExists, db.service_create,
    #                       self.ctxt, values)

    # def test_service_topic_exists_exceptions(self):
    #     db.service_create(self.ctxt, self._get_base_values())
    #     values = self._get_base_values()
    #     values.update({'binary': 'bin1'})
    #     self.assertRaises(exception.ServiceTopicExists, db.service_create,
    #                       self.ctxt, values)

if __name__ == '__main__':
    unittest.main()
