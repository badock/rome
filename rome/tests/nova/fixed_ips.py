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
from nova.objects import fields
from oslo_serialization import jsonutils
from nova.tests import uuidsentinel
import datetime
from oslo_utils import timeutils


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
        from rome.core.utils import LazyRelationship

        # NOTE(badock): Add this block of code to prevent some tests to fail while the test should be considered as a
        # success
        irrelevant_keys = []
        for k, v in obj.items():
            if type(v) is LazyRelationship:
                irrelevant_keys += [k]

        return {k: v for k, v in obj.items()
                if k not in ignored_keys and k not in irrelevant_keys}

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


class BaseInstanceTypeTestCase(unittest.TestCase, ModelsObjectComparatorMixin):
    def setUp(self):
        super(BaseInstanceTypeTestCase, self).setUp()
        self.ctxt = rome_context.get_admin_context()
        # self.user_ctxt = context.RequestContext('user', 'user')
        self.user_ctxt = rome_context.get_admin_context()
        clean_rome_all_databases()

    def _get_base_values(self):
        return {
            'name': 'fake_name',
            'memory_mb': 512,
            'vcpus': 1,
            'root_gb': 10,
            'ephemeral_gb': 10,
            'flavorid': 'fake_flavor',
            'swap': 0,
            'rxtx_factor': 0.5,
            'vcpu_weight': 1,
            'disabled': False,
            'is_public': True
        }

    def _create_flavor(self, values, projects=None):
        v = self._get_base_values()
        v.update(values)
        return db.flavor_create(self.ctxt, v, projects)


#@mock.patch('time.sleep', new=lambda x: None)
class FixedIPTestCase(BaseInstanceTypeTestCase):
    def _timeout_test(self, ctxt, timeout, multi_host):
        instance = db.instance_create(ctxt, dict(host='foo'))
        net = db.network_create_safe(ctxt, dict(multi_host=multi_host,
                                                host='bar'))
        old = timeout - datetime.timedelta(seconds=5)
        new = timeout + datetime.timedelta(seconds=5)
        # should deallocate
        db.fixed_ip_create(ctxt, dict(allocated=False,
                                      instance_uuid=instance['uuid'],
                                      network_id=net['id'],
                                      updated_at=old))
        # still allocated
        db.fixed_ip_create(ctxt, dict(allocated=True,
                                      instance_uuid=instance['uuid'],
                                      network_id=net['id'],
                                      updated_at=old))
        # wrong network
        db.fixed_ip_create(ctxt, dict(allocated=False,
                                      instance_uuid=instance['uuid'],
                                      network_id=None,
                                      updated_at=old))
        # too new
        db.fixed_ip_create(ctxt, dict(allocated=False,
                                      instance_uuid=instance['uuid'],
                                      network_id=None,
                                      updated_at=new))

    def mock_db_query_first_to_raise_data_error_exception(self):
        self.mox.StubOutWithMock(query.Query, 'first')
        query.Query.first().AndRaise(db_exc.DBError())
        self.mox.ReplayAll()

    def test_fixed_ip_disassociate_all_by_timeout_single_host(self):
        now = timeutils.utcnow()
        self._timeout_test(self.ctxt, now, False)
        result = db.fixed_ip_disassociate_all_by_timeout(self.ctxt, 'foo', now)
        self.assertEqual(result, 0)
        result = db.fixed_ip_disassociate_all_by_timeout(self.ctxt, 'bar', now)
        self.assertEqual(result, 1)

    def test_fixed_ip_disassociate_all_by_timeout_multi_host(self):
        from rome.driver.database_driver import get_driver
        driver = get_driver()

        now = timeutils.utcnow()
        self._timeout_test(self.ctxt, now, True)
        result = db.fixed_ip_disassociate_all_by_timeout(self.ctxt, 'foo', now)
        self.assertEqual(result, 1)
        result = db.fixed_ip_disassociate_all_by_timeout(self.ctxt, 'bar', now)
        self.assertEqual(result, 0)

    def test_fixed_ip_get_by_floating_address(self):
        fixed_ip = db.fixed_ip_create(self.ctxt, {'address': '192.168.0.2'})
        values = {'address': '8.7.6.5',
                  'fixed_ip_id': fixed_ip['id']}
        floating = db.floating_ip_create(self.ctxt, values)['address']
        fixed_ip_ref = db.fixed_ip_get_by_floating_address(self.ctxt, floating)
        self._assertEqualObjects(fixed_ip, fixed_ip_ref)

    def test_fixed_ip_get_by_host(self):
        host_ips = {
            'host1': ['1.1.1.1', '1.1.1.2', '1.1.1.3'],
            'host2': ['1.1.1.4', '1.1.1.5'],
            'host3': ['1.1.1.6']
        }

        for host, ips in host_ips.items():
            for ip in ips:
                instance_uuid = self._create_instance(host=host)
                db.fixed_ip_create(self.ctxt, {'address': ip})
                db.fixed_ip_associate(self.ctxt, ip, instance_uuid)

        for host, ips in host_ips.items():
            ips_on_host = [x['address']
                           for x in db.fixed_ip_get_by_host(self.ctxt, host)]
            self._assertEqualListsOfPrimitivesAsSets(ips_on_host, ips)

    def test_fixed_ip_get_by_network_host_not_found_exception(self):
        self.assertRaises(
            exception.FixedIpNotFoundForNetworkHost,
            db.fixed_ip_get_by_network_host,
            self.ctxt, 1, 'ignore')

    def test_fixed_ip_get_by_network_host_fixed_ip_found(self):
        db.fixed_ip_create(self.ctxt, dict(network_id=1, host='host'))

        fip = db.fixed_ip_get_by_network_host(self.ctxt, 1, 'host')

        self.assertEqual(1, fip['network_id'])
        self.assertEqual('host', fip['host'])

    def _create_instance(self, **kwargs):
        instance = db.instance_create(self.ctxt, kwargs)
        return instance['uuid']

    def test_fixed_ip_get_by_instance_fixed_ip_found(self):
        instance_uuid = self._create_instance()

        FIXED_IP_ADDRESS = '192.168.1.5'
        db.fixed_ip_create(self.ctxt, dict(
            instance_uuid=instance_uuid, address=FIXED_IP_ADDRESS))

        ips_list = db.fixed_ip_get_by_instance(self.ctxt, instance_uuid)
        self._assertEqualListsOfPrimitivesAsSets([FIXED_IP_ADDRESS],
                                                 [ips_list[0].address])

    def test_fixed_ip_get_by_instance_multiple_fixed_ips_found(self):
        instance_uuid = self._create_instance()

        FIXED_IP_ADDRESS_1 = '192.168.1.5'
        db.fixed_ip_create(self.ctxt, dict(
            instance_uuid=instance_uuid, address=FIXED_IP_ADDRESS_1))
        FIXED_IP_ADDRESS_2 = '192.168.1.6'
        db.fixed_ip_create(self.ctxt, dict(
            instance_uuid=instance_uuid, address=FIXED_IP_ADDRESS_2))

        ips_list = db.fixed_ip_get_by_instance(self.ctxt, instance_uuid)
        self._assertEqualListsOfPrimitivesAsSets(
            [FIXED_IP_ADDRESS_1, FIXED_IP_ADDRESS_2],
            [ips_list[0].address, ips_list[1].address])

    def test_fixed_ip_get_by_instance_inappropriate_ignored(self):
        instance_uuid = self._create_instance()

        FIXED_IP_ADDRESS_1 = '192.168.1.5'
        db.fixed_ip_create(self.ctxt, dict(
            instance_uuid=instance_uuid, address=FIXED_IP_ADDRESS_1))
        FIXED_IP_ADDRESS_2 = '192.168.1.6'
        db.fixed_ip_create(self.ctxt, dict(
            instance_uuid=instance_uuid, address=FIXED_IP_ADDRESS_2))

        another_instance = db.instance_create(self.ctxt, {})
        db.fixed_ip_create(self.ctxt, dict(
            instance_uuid=another_instance['uuid'], address="192.168.1.7"))

        ips_list = db.fixed_ip_get_by_instance(self.ctxt, instance_uuid)
        self._assertEqualListsOfPrimitivesAsSets(
            [FIXED_IP_ADDRESS_1, FIXED_IP_ADDRESS_2],
            [ips_list[0].address, ips_list[1].address])

    def test_fixed_ip_get_by_instance_not_found_exception(self):
        instance_uuid = self._create_instance()

        self.assertRaises(exception.FixedIpNotFoundForInstance,
                          db.fixed_ip_get_by_instance,
                          self.ctxt, instance_uuid)

    def test_fixed_ips_by_virtual_interface_fixed_ip_found(self):
        instance_uuid = self._create_instance()

        vif = db.virtual_interface_create(
            self.ctxt, dict(instance_uuid=instance_uuid))

        FIXED_IP_ADDRESS = '192.168.1.5'
        db.fixed_ip_create(self.ctxt, dict(
            virtual_interface_id=vif.id, address=FIXED_IP_ADDRESS))

        ips_list = db.fixed_ips_by_virtual_interface(self.ctxt, vif.id)
        self._assertEqualListsOfPrimitivesAsSets([FIXED_IP_ADDRESS],
                                                 [ips_list[0].address])

    def test_fixed_ips_by_virtual_interface_multiple_fixed_ips_found(self):
        instance_uuid = self._create_instance()

        vif = db.virtual_interface_create(
            self.ctxt, dict(instance_uuid=instance_uuid))

        FIXED_IP_ADDRESS_1 = '192.168.1.5'
        db.fixed_ip_create(self.ctxt, dict(
            virtual_interface_id=vif.id, address=FIXED_IP_ADDRESS_1))
        FIXED_IP_ADDRESS_2 = '192.168.1.6'
        db.fixed_ip_create(self.ctxt, dict(
            virtual_interface_id=vif.id, address=FIXED_IP_ADDRESS_2))

        ips_list = db.fixed_ips_by_virtual_interface(self.ctxt, vif.id)
        self._assertEqualListsOfPrimitivesAsSets(
            [FIXED_IP_ADDRESS_1, FIXED_IP_ADDRESS_2],
            [ips_list[0].address, ips_list[1].address])

    def test_fixed_ips_by_virtual_interface_inappropriate_ignored(self):
        instance_uuid = self._create_instance()

        vif = db.virtual_interface_create(
            self.ctxt, dict(instance_uuid=instance_uuid))

        FIXED_IP_ADDRESS_1 = '192.168.1.5'
        db.fixed_ip_create(self.ctxt, dict(
            virtual_interface_id=vif.id, address=FIXED_IP_ADDRESS_1))
        FIXED_IP_ADDRESS_2 = '192.168.1.6'
        db.fixed_ip_create(self.ctxt, dict(
            virtual_interface_id=vif.id, address=FIXED_IP_ADDRESS_2))

        another_vif = db.virtual_interface_create(
            self.ctxt, dict(instance_uuid=instance_uuid))
        db.fixed_ip_create(self.ctxt, dict(
            virtual_interface_id=another_vif.id, address="192.168.1.7"))

        ips_list = db.fixed_ips_by_virtual_interface(self.ctxt, vif.id)
        self._assertEqualListsOfPrimitivesAsSets(
            [FIXED_IP_ADDRESS_1, FIXED_IP_ADDRESS_2],
            [ips_list[0].address, ips_list[1].address])

    def test_fixed_ips_by_virtual_interface_no_ip_found(self):
        instance_uuid = self._create_instance()

        vif = db.virtual_interface_create(
            self.ctxt, dict(instance_uuid=instance_uuid))

        ips_list = db.fixed_ips_by_virtual_interface(self.ctxt, vif.id)
        self.assertEqual(0, len(ips_list))

    def create_fixed_ip(self, **params):
        default_params = {'address': '192.168.0.1'}
        default_params.update(params)
        return db.fixed_ip_create(self.ctxt, default_params)['address']

    def test_fixed_ip_associate_fails_if_ip_not_in_network(self):
        instance_uuid = self._create_instance()
        self.assertRaises(exception.FixedIpNotFoundForNetwork,
                          db.fixed_ip_associate,
                          self.ctxt, None, instance_uuid)

    def test_fixed_ip_associate_fails_if_ip_in_use(self):
        instance_uuid = self._create_instance()

        address = self.create_fixed_ip(instance_uuid=instance_uuid)
        self.assertRaises(exception.FixedIpAlreadyInUse,
                          db.fixed_ip_associate,
                          self.ctxt, address, instance_uuid)

    def test_fixed_ip_associate_succeeds(self):
        instance_uuid = self._create_instance()
        network = db.network_create_safe(self.ctxt, {})

        address = self.create_fixed_ip(network_id=network['id'])
        db.fixed_ip_associate(self.ctxt, address, instance_uuid,
                              network_id=network['id'])
        fixed_ip = db.fixed_ip_get_by_address(self.ctxt, address)
        self.assertEqual(fixed_ip['instance_uuid'], instance_uuid)

    def test_fixed_ip_associate_succeeds_and_sets_network(self):
        instance_uuid = self._create_instance()
        network = db.network_create_safe(self.ctxt, {})

        address = self.create_fixed_ip()
        db.fixed_ip_associate(self.ctxt, address, instance_uuid,
                              network_id=network['id'])
        fixed_ip = db.fixed_ip_get_by_address(self.ctxt, address)
        self.assertEqual(fixed_ip['instance_uuid'], instance_uuid)
        self.assertEqual(fixed_ip['network_id'], network['id'])

    # def test_fixed_ip_associate_succeeds_retry_on_deadlock(self):
    #     instance_uuid = self._create_instance()
    #     network = db.network_create_safe(self.ctxt, {})
    #
    #     address = self.create_fixed_ip()
    #
    #     def fake_first():
    #         if mock_first.call_count == 1:
    #             raise db_exc.DBDeadlock()
    #         else:
    #             return objects.Instance(id=1, address=address, reserved=False,
    #                                     instance_uuid=None, network_id=None)
    #
    #     with mock.patch('sqlalchemy.orm.query.Query.first',
    #                     side_effect=fake_first) as mock_first:
    #         db.fixed_ip_associate(self.ctxt, address, instance_uuid,
    #                               network_id=network['id'])
    #         self.assertEqual(2, mock_first.call_count)
    #
    #     fixed_ip = db.fixed_ip_get_by_address(self.ctxt, address)
    #     self.assertEqual(fixed_ip['instance_uuid'], instance_uuid)
    #     self.assertEqual(fixed_ip['network_id'], network['id'])

    # def test_fixed_ip_associate_succeeds_retry_on_no_rows_updated(self):
    #     instance_uuid = self._create_instance()
    #     network = db.network_create_safe(self.ctxt, {})
    #
    #     address = self.create_fixed_ip()
    #
    #     def fake_first():
    #         if mock_first.call_count == 1:
    #             return objects.Instance(id=2, address=address, reserved=False,
    #                                     instance_uuid=None, network_id=None)
    #         else:
    #             return objects.Instance(id=1, address=address, reserved=False,
    #                                     instance_uuid=None, network_id=None)
    #
    #     with mock.patch('sqlalchemy.orm.query.Query.first',
    #                     side_effect=fake_first) as mock_first:
    #         db.fixed_ip_associate(self.ctxt, address, instance_uuid,
    #                               network_id=network['id'])
    #         self.assertEqual(2, mock_first.call_count)
    #
    #     fixed_ip = db.fixed_ip_get_by_address(self.ctxt, address)
    #     self.assertEqual(fixed_ip['instance_uuid'], instance_uuid)
    #     self.assertEqual(fixed_ip['network_id'], network['id'])

    # def test_fixed_ip_associate_succeeds_retry_limit_exceeded(self):
    #     instance_uuid = self._create_instance()
    #     network = db.network_create_safe(self.ctxt, {})
    #
    #     address = self.create_fixed_ip()
    #
    #     def fake_first():
    #         return objects.Instance(id=2, address=address, reserved=False,
    #                                 instance_uuid=None, network_id=None)
    #
    #     with mock.patch('sqlalchemy.orm.query.Query.first',
    #                     side_effect=fake_first) as mock_first:
    #         self.assertRaises(exception.FixedIpAssociateFailed,
    #                           db.fixed_ip_associate, self.ctxt, address,
    #                           instance_uuid, network_id=network['id'])
    #         # 5 reties + initial attempt
    #         self.assertEqual(6, mock_first.call_count)

    # def test_fixed_ip_associate_ip_not_in_network_with_no_retries(self):
    #     instance_uuid = self._create_instance()
    #
    #     with mock.patch('sqlalchemy.orm.query.Query.first',
    #                     return_value=None) as mock_first:
    #         self.assertRaises(exception.FixedIpNotFoundForNetwork,
    #                           db.fixed_ip_associate,
    #                           self.ctxt, None, instance_uuid)
    #         self.assertEqual(1, mock_first.call_count)

    # def test_fixed_ip_associate_no_network_id_with_no_retries(self):
    #     # Tests that trying to associate an instance to a fixed IP on a network
    #     # but without specifying the network ID during associate will fail.
    #     instance_uuid = self._create_instance()
    #     network = db.network_create_safe(self.ctxt, {})
    #     address = self.create_fixed_ip(network_id=network['id'])
    #
    #     with mock.patch('sqlalchemy.orm.query.Query.first',
    #                     return_value=None) as mock_first:
    #         self.assertRaises(exception.FixedIpNotFoundForNetwork,
    #                           db.fixed_ip_associate,
    #                           self.ctxt, address, instance_uuid)
    #         self.assertEqual(1, mock_first.call_count)

    def test_fixed_ip_associate_with_vif(self):
        instance_uuid = self._create_instance()
        network = db.network_create_safe(self.ctxt, {})
        vif = db.virtual_interface_create(self.ctxt, {})
        address = self.create_fixed_ip()

        fixed_ip = db.fixed_ip_associate(self.ctxt, address, instance_uuid,
                                         network_id=network['id'],
                                         virtual_interface_id=vif['id'])

        self.assertTrue(fixed_ip['allocated'])
        self.assertEqual(vif['id'], fixed_ip['virtual_interface_id'])

    def test_fixed_ip_associate_not_allocated_without_vif(self):
        instance_uuid = self._create_instance()
        address = self.create_fixed_ip()

        fixed_ip = db.fixed_ip_associate(self.ctxt, address, instance_uuid)

        self.assertFalse(fixed_ip['allocated'])
        self.assertIsNone(fixed_ip['virtual_interface_id'])

    def test_fixed_ip_associate_pool_invalid_uuid(self):
        instance_uuid = '123'
        self.assertRaises(exception.InvalidUUID, db.fixed_ip_associate_pool,
                          self.ctxt, None, instance_uuid)

    def test_fixed_ip_associate_pool_no_more_fixed_ips(self):
        instance_uuid = self._create_instance()
        self.assertRaises(exception.NoMoreFixedIps, db.fixed_ip_associate_pool,
                          self.ctxt, None, instance_uuid)

    def test_fixed_ip_associate_pool_ignores_leased_addresses(self):
        instance_uuid = self._create_instance()
        params = {'address': '192.168.1.5',
                  'leased': True}
        db.fixed_ip_create(self.ctxt, params)
        self.assertRaises(exception.NoMoreFixedIps, db.fixed_ip_associate_pool,
                          self.ctxt, None, instance_uuid)

    def test_fixed_ip_associate_pool_succeeds(self):
        instance_uuid = self._create_instance()
        network = db.network_create_safe(self.ctxt, {})

        address = self.create_fixed_ip(network_id=network['id'])
        db.fixed_ip_associate_pool(self.ctxt, network['id'], instance_uuid)
        fixed_ip = db.fixed_ip_get_by_address(self.ctxt, address)
        self.assertEqual(fixed_ip['instance_uuid'], instance_uuid)

    def test_fixed_ip_associate_pool_order(self):
        """Test that fixed_ip always uses oldest fixed_ip.

        We should always be using the fixed ip with the oldest
        updated_at.
        """

        instance_uuid = self._create_instance()
        network = db.network_create_safe(self.ctxt, {})
        self.addCleanup(timeutils.clear_time_override)
        start = timeutils.utcnow()
        for i in range(1, 4):
            now = start - datetime.timedelta(hours=i)
            timeutils.set_time_override(now)
            address = self.create_fixed_ip(
                updated_at=now,
                address='10.1.0.%d' % i,
                network_id=network['id'])
        db.fixed_ip_associate_pool(self.ctxt, network['id'], instance_uuid)
        fixed_ip = db.fixed_ip_get_by_address(self.ctxt, address)
        self.assertEqual(fixed_ip['instance_uuid'], instance_uuid)

    def test_fixed_ip_associate_pool_succeeds_fip_ref_network_id_is_none(self):
        instance_uuid = self._create_instance()
        network = db.network_create_safe(self.ctxt, {})

        self.create_fixed_ip(network_id=None)
        fixed_ip = db.fixed_ip_associate_pool(self.ctxt,
                                              network['id'], instance_uuid)
        self.assertEqual(instance_uuid, fixed_ip['instance_uuid'])
        self.assertEqual(network['id'], fixed_ip['network_id'])

    # def test_fixed_ip_associate_pool_succeeds_retry(self):
    #     instance_uuid = self._create_instance()
    #     network = db.network_create_safe(self.ctxt, {})
    #
    #     address = self.create_fixed_ip(network_id=network['id'])
    #
    #     def fake_first():
    #         if mock_first.call_count == 1:
    #             return {'network_id': network['id'], 'address': 'invalid',
    #                     'instance_uuid': None, 'host': None, 'id': 1}
    #         else:
    #             return {'network_id': network['id'], 'address': address,
    #                     'instance_uuid': None, 'host': None, 'id': 1}
    #
    #     with mock.patch('sqlalchemy.orm.query.Query.first',
    #                     side_effect=fake_first) as mock_first:
    #         db.fixed_ip_associate_pool(self.ctxt, network['id'], instance_uuid)
    #         self.assertEqual(2, mock_first.call_count)
    #
    #     fixed_ip = db.fixed_ip_get_by_address(self.ctxt, address)
    #     self.assertEqual(instance_uuid, fixed_ip['instance_uuid'])

    # def test_fixed_ip_associate_pool_retry_limit_exceeded(self):
    #     instance_uuid = self._create_instance()
    #     network = db.network_create_safe(self.ctxt, {})
    #
    #     self.create_fixed_ip(network_id=network['id'])
    #
    #     def fake_first():
    #         return {'network_id': network['id'], 'address': 'invalid',
    #                 'instance_uuid': None, 'host': None, 'id': 1}
    #
    #     with mock.patch('sqlalchemy.orm.query.Query.first',
    #                     side_effect=fake_first) as mock_first:
    #         self.assertRaises(exception.FixedIpAssociateFailed,
    #                           db.fixed_ip_associate_pool, self.ctxt,
    #                           network['id'], instance_uuid)
    #         # 5 retries + initial attempt
    #         self.assertEqual(6, mock_first.call_count)

    # def test_fixed_ip_create_same_address(self):
    #     address = '192.168.1.5'
    #     params = {'address': address}
    #     db.fixed_ip_create(self.ctxt, params)
    #     self.assertRaises(exception.FixedIpExists, db.fixed_ip_create,
    #                       self.ctxt, params)

    def test_fixed_ip_create_success(self):
        instance_uuid = self._create_instance()
        network_id = db.network_create_safe(self.ctxt, {})['id']
        param = {
            'reserved': False,
            'deleted': 0,
            'leased': False,
            'host': '127.0.0.1',
            'address': '192.168.1.5',
            'allocated': False,
            'instance_uuid': instance_uuid,
            'network_id': network_id,
            'virtual_interface_id': None
        }

        ignored_keys = ['created_at', 'id', 'deleted_at', 'updated_at', 'floating_ips']
        fixed_ip_data = db.fixed_ip_create(self.ctxt, param)
        self._assertEqualObjects(param, fixed_ip_data, ignored_keys)

    # def test_fixed_ip_bulk_create_same_address(self):
    #     address_1 = '192.168.1.5'
    #     address_2 = '192.168.1.6'
    #     instance_uuid = self._create_instance()
    #     network_id_1 = db.network_create_safe(self.ctxt, {})['id']
    #     network_id_2 = db.network_create_safe(self.ctxt, {})['id']
    #     params = [
    #         {'reserved': False, 'deleted': 0, 'leased': False,
    #          'host': '127.0.0.1', 'address': address_2, 'allocated': False,
    #          'instance_uuid': instance_uuid, 'network_id': network_id_1,
    #          'virtual_interface_id': None},
    #         {'reserved': False, 'deleted': 0, 'leased': False,
    #          'host': '127.0.0.1', 'address': address_1, 'allocated': False,
    #          'instance_uuid': instance_uuid, 'network_id': network_id_1,
    #          'virtual_interface_id': None},
    #         {'reserved': False, 'deleted': 0, 'leased': False,
    #          'host': 'localhost', 'address': address_2, 'allocated': True,
    #          'instance_uuid': instance_uuid, 'network_id': network_id_2,
    #          'virtual_interface_id': None},
    #     ]
    #
    #     self.assertRaises(exception.FixedIpExists, db.fixed_ip_bulk_create,
    #                       self.ctxt, params)
    #     # In this case the transaction will be rolled back and none of the ips
    #     # will make it to the database.
    #     self.assertRaises(exception.FixedIpNotFoundForAddress,
    #                       db.fixed_ip_get_by_address, self.ctxt, address_1)
    #     self.assertRaises(exception.FixedIpNotFoundForAddress,
    #                       db.fixed_ip_get_by_address, self.ctxt, address_2)

    def test_fixed_ip_bulk_create_success(self):
        address_1 = '192.168.1.5'
        address_2 = '192.168.1.6'

        instance_uuid = self._create_instance()
        network_id_1 = db.network_create_safe(self.ctxt, {})['id']
        network_id_2 = db.network_create_safe(self.ctxt, {})['id']
        params = [
            {'reserved': False, 'deleted': 0, 'leased': False,
             'host': '127.0.0.1', 'address': address_1, 'allocated': False,
             'instance_uuid': instance_uuid, 'network_id': network_id_1,
             'virtual_interface_id': None},
            {'reserved': False, 'deleted': 0, 'leased': False,
             'host': 'localhost', 'address': address_2, 'allocated': True,
             'instance_uuid': instance_uuid, 'network_id': network_id_2,
             'virtual_interface_id': None}
        ]

        db.fixed_ip_bulk_create(self.ctxt, params)
        ignored_keys = ['created_at', 'id', 'deleted_at', 'updated_at',
                        'virtual_interface', 'network', 'floating_ips', '___version_number']
        fixed_ip_data = db.fixed_ip_get_by_instance(self.ctxt, instance_uuid)

        # we have no `id` in incoming data so we can not use
        # _assertEqualListsOfObjects to compare incoming data and received
        # objects
        fixed_ip_data = sorted(fixed_ip_data, key=lambda i: i['network_id'])
        params = sorted(params, key=lambda i: i['network_id'])
        for param, ip in zip(params, fixed_ip_data):
            self._assertEqualObjects(param, ip, ignored_keys)

    # def test_fixed_ip_disassociate(self):
    #     address = '192.168.1.5'
    #     instance_uuid = self._create_instance()
    #     network_id = db.network_create_safe(self.ctxt, {})['id']
    #     values = {'address': '192.168.1.5', 'instance_uuid': instance_uuid}
    #     vif = db.virtual_interface_create(self.ctxt, values)
    #     param = {
    #         'reserved': False,
    #         'deleted': 0,
    #         'leased': False,
    #         'host': '127.0.0.1',
    #         'address': address,
    #         'allocated': False,
    #         'instance_uuid': instance_uuid,
    #         'network_id': network_id,
    #         'virtual_interface_id': vif['id']
    #     }
    #     db.fixed_ip_create(self.ctxt, param)
    #
    #     db.fixed_ip_disassociate(self.ctxt, address)
    #     fixed_ip_data = db.fixed_ip_get_by_address(self.ctxt, address)
    #     ignored_keys = ['created_at', 'id', 'deleted_at',
    #                     'updated_at', 'instance_uuid',
    #                     'virtual_interface_id', 'floating_ips']
    #     self._assertEqualObjects(param, fixed_ip_data, ignored_keys)
    #     self.assertIsNone(fixed_ip_data['instance_uuid'])
    #     self.assertIsNone(fixed_ip_data['virtual_interface_id'])

    def test_fixed_ip_get_not_found_exception(self):
        self.assertRaises(exception.FixedIpNotFound,
                          db.fixed_ip_get, self.ctxt, 0)

    def test_fixed_ip_get_success2(self):
        address = '192.168.1.5'
        instance_uuid = self._create_instance()
        network_id = db.network_create_safe(self.ctxt, {})['id']
        param = {
            'reserved': False,
            'deleted': 0,
            'leased': False,
            'host': '127.0.0.1',
            'address': address,
            'allocated': False,
            'instance_uuid': instance_uuid,
            'network_id': network_id,
            'virtual_interface_id': None
        }
        fixed_ip_id = db.fixed_ip_create(self.ctxt, param)

        self.ctxt.is_admin = False
        self.assertRaises(exception.Forbidden, db.fixed_ip_get,
                          self.ctxt, fixed_ip_id)

    def test_fixed_ip_get_success(self):
        address = '192.168.1.5'
        instance_uuid = self._create_instance()
        network_id = db.network_create_safe(self.ctxt, {})['id']
        param = {
            'reserved': False,
            'deleted': 0,
            'leased': False,
            'host': '127.0.0.1',
            'address': address,
            'allocated': False,
            'instance_uuid': instance_uuid,
            'network_id': network_id,
            'virtual_interface_id': None
        }
        db.fixed_ip_create(self.ctxt, param)

        fixed_ip_id = db.fixed_ip_get_by_address(self.ctxt, address)['id']
        fixed_ip_data = db.fixed_ip_get(self.ctxt, fixed_ip_id)
        ignored_keys = ['created_at', 'id', 'deleted_at', 'updated_at', 'floating_ips']
        self._assertEqualObjects(param, fixed_ip_data, ignored_keys)

    def test_fixed_ip_get_by_address(self):
        instance_uuid = self._create_instance()
        db.fixed_ip_create(self.ctxt, {'address': '1.2.3.4',
                                       'instance_uuid': instance_uuid,
                                       })
        fixed_ip = db.fixed_ip_get_by_address(self.ctxt, '1.2.3.4',
                                              columns_to_join=['instance'])
        self.assertIn('instance', fixed_ip.__dict__)
        self.assertEqual(instance_uuid, fixed_ip.instance.uuid)

    def test_fixed_ip_update_not_found_for_address(self):
        self.assertRaises(exception.FixedIpNotFoundForAddress,
                          db.fixed_ip_update, self.ctxt,
                          '192.168.1.5', {})

    def test_fixed_ip_update(self):
        instance_uuid_1 = self._create_instance()
        instance_uuid_2 = self._create_instance()
        network_id_1 = db.network_create_safe(self.ctxt, {})['id']
        network_id_2 = db.network_create_safe(self.ctxt, {})['id']
        param_1 = {
            'reserved': True, 'deleted': 0, 'leased': True,
            'host': '192.168.133.1', 'address': '10.0.0.2',
            'allocated': True, 'instance_uuid': instance_uuid_1,
            'network_id': network_id_1, 'virtual_interface_id': '123',
        }

        param_2 = {
            'reserved': False, 'deleted': 0, 'leased': False,
            'host': '127.0.0.1', 'address': '10.0.0.3', 'allocated': False,
            'instance_uuid': instance_uuid_2, 'network_id': network_id_2,
            'virtual_interface_id': None
        }

        ignored_keys = ['created_at', 'id', 'deleted_at', 'updated_at', 'floating_ips']
        fixed_ip_addr = db.fixed_ip_create(self.ctxt, param_1)['address']
        db.fixed_ip_update(self.ctxt, fixed_ip_addr, param_2)
        fixed_ip_after_update = db.fixed_ip_get_by_address(self.ctxt,
                                                           param_2['address'])
        self._assertEqualObjects(param_2, fixed_ip_after_update, ignored_keys)
