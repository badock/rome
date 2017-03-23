# import logging
import unittest

# from models import *
from nova import exception
import api as db
# from oslo_serialization import jsonutils
# from nova.tests import uuidsentinel
# from nova import test
from utils import clean_rome_all_databases
# from nova.tests.unit import matchers
# import mock
# from oslo_db import exception as db_exc
#
# from nova.objects import fields
# import copy
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


class NetworkTestCase(unittest.TestCase, ModelsObjectComparatorMixin):

    """Tests for db.api.network_* methods."""

    def setUp(self):
        super(NetworkTestCase, self).setUp()
        self.ctxt = rome_context.get_admin_context()
        clean_rome_all_databases()


    def _get_associated_fixed_ip(self, host, cidr, ip):
        network = db.network_create_safe(self.ctxt,
            {'project_id': 'project1', 'cidr': cidr})
        self.assertFalse(db.network_in_use_on_host(self.ctxt, network.id,
            host))
        instance = db.instance_create(self.ctxt,
            {'project_id': 'project1', 'host': host})
        virtual_interface = db.virtual_interface_create(self.ctxt,
            {'instance_uuid': instance.uuid, 'network_id': network.id,
            'address': ip})
        db.fixed_ip_create(self.ctxt, {'address': ip,
            'network_id': network.id, 'allocated': True,
            'virtual_interface_id': virtual_interface.id})
        db.fixed_ip_associate(self.ctxt, ip, instance.uuid,
            network.id, virtual_interface_id=virtual_interface['id'])
        return network, instance

    # def test_network_get_associated_default_route(self):
    #     network, instance = self._get_associated_fixed_ip('host.net',
    #         '192.0.2.0/30', '192.0.2.1')
    #     network2 = db.network_create_safe(self.ctxt,
    #         {'project_id': 'project1', 'cidr': '192.0.3.0/30'})
    #     ip = '192.0.3.1'
    #     virtual_interface = db.virtual_interface_create(self.ctxt,
    #         {'instance_uuid': instance.uuid, 'network_id': network2.id,
    #         'address': ip})
    #     db.fixed_ip_create(self.ctxt, {'address': ip,
    #         'network_id': network2.id, 'allocated': True,
    #         'virtual_interface_id': virtual_interface.id})
    #     db.fixed_ip_associate(self.ctxt, ip, instance.uuid,
    #         network2.id)
    #     data = db.network_get_associated_fixed_ips(self.ctxt, network.id)
    #     self.assertEqual(1, len(data))
    #     self.assertTrue(data[0]['default_route'])
    #     data = db.network_get_associated_fixed_ips(self.ctxt, network2.id)
    #     self.assertEqual(1, len(data))
    #     self.assertFalse(data[0]['default_route'])

    # def test_network_get_associated_fixed_ips(self):
    #     network, instance = self._get_associated_fixed_ip('host.net',
    #         '192.0.2.0/30', '192.0.2.1')
    #     data = db.network_get_associated_fixed_ips(self.ctxt, network.id)
    #     self.assertEqual(1, len(data))
    #     self.assertEqual('192.0.2.1', data[0]['address'])
    #     self.assertEqual('192.0.2.1', data[0]['vif_address'])
    #     self.assertEqual(instance.uuid, data[0]['instance_uuid'])
    #     self.assertTrue(data[0][fields.PciDeviceStatus.ALLOCATED])

    def test_network_create_safe(self):
        values = {'host': 'localhost', 'project_id': 'project1'}
        network = db.network_create_safe(self.ctxt, values)
        self.assertEqual(36, len(network['uuid']))
        db_network = db.network_get(self.ctxt, network['id'])
        self._assertEqualObjects(network, db_network)

    # def test_network_create_with_duplicate_vlan(self):
    #     values1 = {'host': 'localhost', 'project_id': 'project1', 'vlan': 1}
    #     values2 = {'host': 'something', 'project_id': 'project1', 'vlan': 1}
    #     db.network_create_safe(self.ctxt, values1)
    #     self.assertRaises(exception.DuplicateVlan,
    #                       db.network_create_safe, self.ctxt, values2)

    # def test_network_delete_safe(self):
    #     values = {'host': 'localhost', 'project_id': 'project1'}
    #     network = db.network_create_safe(self.ctxt, values)
    #     db.network_get(self.ctxt, network['id'])
    #     values = {'network_id': network['id'], 'address': '192.168.1.5'}
    #     address1 = db.fixed_ip_create(self.ctxt, values)['address']
    #     values = {'network_id': network['id'],
    #               'address': '192.168.1.6',
    #               'allocated': True}
    #     address2 = db.fixed_ip_create(self.ctxt, values)['address']
    #     self.assertRaises(exception.NetworkInUse,
    #                       db.network_delete_safe, self.ctxt, network['id'])
    #     db.fixed_ip_update(self.ctxt, address2, {'allocated': False})
    #     network = db.network_delete_safe(self.ctxt, network['id'])
    #     self.assertRaises(exception.FixedIpNotFoundForAddress,
    #                       db.fixed_ip_get_by_address, self.ctxt, address1)
    #     ctxt = self.ctxt.elevated(read_deleted='yes')
    #     fixed_ip = db.fixed_ip_get_by_address(ctxt, address1)
    #     self.assertTrue(fixed_ip['deleted'])

    # def test_network_in_use_on_host(self):
    #     values = {'host': 'foo', 'hostname': 'myname'}
    #     instance = db.instance_create(self.ctxt, values)
    #     values = {'address': '192.168.1.5', 'instance_uuid': instance['uuid']}
    #     vif = db.virtual_interface_create(self.ctxt, values)
    #     values = {'address': '192.168.1.6',
    #               'network_id': 1,
    #               'allocated': True,
    #               'instance_uuid': instance['uuid'],
    #               'virtual_interface_id': vif['id']}
    #     db.fixed_ip_create(self.ctxt, values)
    #     self.assertTrue(db.network_in_use_on_host(self.ctxt, 1, 'foo'))
    #     self.assertFalse(db.network_in_use_on_host(self.ctxt, 1, 'bar'))

    def test_network_update_nonexistent(self):
        self.assertRaises(exception.NetworkNotFound,
            db.network_update, self.ctxt, 123456, {})

    # def test_network_update_with_duplicate_vlan(self):
    #     values1 = {'host': 'localhost', 'project_id': 'project1', 'vlan': 1}
    #     values2 = {'host': 'something', 'project_id': 'project1', 'vlan': 2}
    #     network_ref = db.network_create_safe(self.ctxt, values1)
    #     db.network_create_safe(self.ctxt, values2)
    #     self.assertRaises(exception.DuplicateVlan,
    #                       db.network_update, self.ctxt,
    #                       network_ref["id"], values2)

    def test_network_update(self):
        network = db.network_create_safe(self.ctxt, {'project_id': 'project1',
            'vlan': 1, 'host': 'test.com'})
        db.network_update(self.ctxt, network.id, {'vlan': 2})
        network_new = db.network_get(self.ctxt, network.id)
        self.assertEqual(2, network_new.vlan)

    def test_network_set_host_nonexistent_network(self):
        self.assertRaises(exception.NetworkNotFound, db.network_set_host,
                          self.ctxt, 123456, 'nonexistent')

    def test_network_set_host_already_set_correct(self):
        values = {'host': 'example.com', 'project_id': 'project1'}
        network = db.network_create_safe(self.ctxt, values)
        self.assertIsNone(db.network_set_host(self.ctxt, network.id,
                          'example.com'))

    def test_network_set_host_already_set_incorrect(self):
        values = {'host': 'example.com', 'project_id': 'project1'}
        network = db.network_create_safe(self.ctxt, values)
        self.assertIsNone(db.network_set_host(self.ctxt, network.id,
                                              'new.example.com'))

    def test_network_set_host_with_initially_no_host(self):
        values = {'project_id': 'project1'}
        network = db.network_create_safe(self.ctxt, values)
        db.network_set_host(self.ctxt, network.id, 'example.com')
        self.assertEqual('example.com',
            db.network_get(self.ctxt, network.id).host)

    # def test_network_set_host_succeeds_retry_on_deadlock(self):
    #     values = {'project_id': 'project1'}
    #     network = db.network_create_safe(self.ctxt, values)
    #
    #     def fake_update(params):
    #         if mock_update.call_count == 1:
    #             raise db_exc.DBDeadlock()
    #         else:
    #             return 1
    #
    #     with mock.patch('sqlalchemy.orm.query.Query.update',
    #                     side_effect=fake_update) as mock_update:
    #         db.network_set_host(self.ctxt, network.id, 'example.com')
    #         self.assertEqual(2, mock_update.call_count)
    #
    # def test_network_set_host_succeeds_retry_on_no_rows_updated(self):
    #     values = {'project_id': 'project1'}
    #     network = db.network_create_safe(self.ctxt, values)
    #
    #     def fake_update(params):
    #         if mock_update.call_count == 1:
    #             return 0
    #         else:
    #             return 1
    #
    #     with mock.patch('sqlalchemy.orm.query.Query.update',
    #                     side_effect=fake_update) as mock_update:
    #         db.network_set_host(self.ctxt, network.id, 'example.com')
    #         self.assertEqual(2, mock_update.call_count)

    # def test_network_set_host_failed_with_retry_on_no_rows_updated(self):
    #     values = {'project_id': 'project1'}
    #     network = db.network_create_safe(self.ctxt, values)
    #
    #     with mock.patch('sqlalchemy.orm.query.Query.update',
    #                     return_value=0) as mock_update:
    #         self.assertRaises(exception.NetworkSetHostFailed,
    #                           db.network_set_host, self.ctxt, network.id,
    #                           'example.com')
    #         # 5 retries + initial attempt
    #         self.assertEqual(6, mock_update.call_count)

    def test_network_get_all_by_host(self):
        from rome.driver.database_driver import get_driver
        driver = get_driver()
        from rome.core.orm.query import Query
        from rome.tests.nova.models import FixedIp, Instance
        self.assertEqual([],
            db.network_get_all_by_host(self.ctxt, 'example.com'))
        host = 'h1.example.com'
        # network with host set
        net1 = db.network_create_safe(self.ctxt, {'host': host})
        self._assertEqualListsOfObjects([net1],
            db.network_get_all_by_host(self.ctxt, host))
        # network with fixed ip with host set
        net2 = db.network_create_safe(self.ctxt, {})
        db.fixed_ip_create(self.ctxt, {'host': host, 'network_id': net2.id})
        db.network_get_all_by_host(self.ctxt, host)
        self._assertEqualListsOfObjects([net1, net2],
            db.network_get_all_by_host(self.ctxt, host), ignored_keys="fixed_ips")
        # network with instance with host set
        net3 = db.network_create_safe(self.ctxt, {})
        instance = db.instance_create(self.ctxt, {'host': host})
        db.fixed_ip_create(self.ctxt, {'network_id': net3.id,
            'instance_uuid': instance.uuid})
        self._assertEqualListsOfObjects([net1, net2, net3],
            db.network_get_all_by_host(self.ctxt, host), ignored_keys="fixed_ips")

    def test_network_get_by_cidr(self):
        cidr = '192.0.2.0/30'
        cidr_v6 = '2001:db8:1::/64'
        network = db.network_create_safe(self.ctxt,
            {'project_id': 'project1', 'cidr': cidr, 'cidr_v6': cidr_v6})
        self._assertEqualObjects(network,
            db.network_get_by_cidr(self.ctxt, cidr))
        self._assertEqualObjects(network,
            db.network_get_by_cidr(self.ctxt, cidr_v6))

    def test_network_get_by_cidr_nonexistent(self):
        self.assertRaises(exception.NetworkNotFoundForCidr,
            db.network_get_by_cidr, self.ctxt, '192.0.2.0/30')

    def test_network_get_by_uuid(self):
        network = db.network_create_safe(self.ctxt,
            {'project_id': 'project_1'})
        self._assertEqualObjects(network,
            db.network_get_by_uuid(self.ctxt, network.uuid))

    def test_network_get_by_uuid_nonexistent(self):
        self.assertRaises(exception.NetworkNotFoundForUUID,
            db.network_get_by_uuid, self.ctxt, 'non-existent-uuid')

    def test_network_get_all_by_uuids_no_networks(self):
        self.assertRaises(exception.NoNetworksFound,
            db.network_get_all_by_uuids, self.ctxt, ['non-existent-uuid'])

    def test_network_get_all_by_uuids(self):
        net1 = db.network_create_safe(self.ctxt, {})
        net2 = db.network_create_safe(self.ctxt, {})
        self._assertEqualListsOfObjects([net1, net2],
            db.network_get_all_by_uuids(self.ctxt, [net1.uuid, net2.uuid]))

    def test_network_get_all_no_networks(self):
        self.assertRaises(exception.NoNetworksFound,
            db.network_get_all, self.ctxt, [])

    def test_network_get_all(self):
        network = db.network_create_safe(self.ctxt, {})
        network_db = db.network_get_all(self.ctxt)
        self.assertEqual(1, len(network_db))
        self._assertEqualObjects(network, network_db[0])

    def test_network_get_all_admin_user(self):
        network1 = db.network_create_safe(self.ctxt, {})
        network2 = db.network_create_safe(self.ctxt,
                                          {'project_id': 'project1'})
        self._assertEqualListsOfObjects([network1, network2],
                                        db.network_get_all(self.ctxt,
                                                           project_only=True))

    # def test_network_get_all_normal_user(self):
    #     # normal_ctxt = context.RequestContext('fake', 'fake')
    #     db.network_create_safe(self.ctxt, {})
    #     db.network_create_safe(self.ctxt, {'project_id': 'project1'})
    #     network1 = db.network_create_safe(self.ctxt,
    #                                       {'project_id': 'fake'})
    #     network_db = db.network_get_all(self.ctxt, project_only=True)
    #     self.assertEqual(1, len(network_db))
    #     self._assertEqualObjects(network1, network_db[0])

    def test_network_get(self):
        network = db.network_create_safe(self.ctxt, {})
        self._assertEqualObjects(db.network_get(self.ctxt, network.id),
            network)
        db.network_delete_safe(self.ctxt, network.id)
        self.assertRaises(exception.NetworkNotFound,
            db.network_get, self.ctxt, network.id)

    def test_network_associate(self):
        network = db.network_create_safe(self.ctxt, {})
        self.assertIsNone(network.project_id)
        db.network_associate(self.ctxt, "project1", network.id)
        self.assertEqual("project1", db.network_get(self.ctxt,
            network.id).project_id)

    def test_network_diassociate(self):
        network = db.network_create_safe(self.ctxt,
            {'project_id': 'project1', 'host': 'test.net'})
        # disassociate project
        db.network_disassociate(self.ctxt, network.id, False, True)
        self.assertIsNone(db.network_get(self.ctxt, network.id).project_id)
        # disassociate host
        db.network_disassociate(self.ctxt, network.id, True, False)
        self.assertIsNone(db.network_get(self.ctxt, network.id).host)

    def test_network_count_reserved_ips(self):
        net = db.network_create_safe(self.ctxt, {})
        self.assertEqual(0, db.network_count_reserved_ips(self.ctxt, net.id))
        db.fixed_ip_create(self.ctxt, {'network_id': net.id,
            'reserved': True})
        self.assertEqual(1, db.network_count_reserved_ips(self.ctxt, net.id))
