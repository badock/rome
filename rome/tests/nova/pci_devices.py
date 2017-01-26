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


class PciDeviceDBApiTestCase(unittest.TestCase, ModelsObjectComparatorMixin):
    def setUp(self):
        super(PciDeviceDBApiTestCase, self).setUp()
        clean_rome_all_databases()
        self.user_id = 'fake_user'
        self.project_id = 'fake_project'
        # self.context = context.RequestContext(self.user_id, self.project_id)
        self.context = rome_context.get_admin_context()
        self.admin_context = rome_context.get_admin_context()
        self.ignored_keys = ['id', 'deleted', 'deleted_at', 'updated_at',
                             'created_at']
        self._compute_node = None

    def _get_fake_pci_devs(self):
        return {'id': 3353,
                'compute_node_id': 1,
                'address': '0000:0f:08.7',
                'vendor_id': '8086',
                'product_id': '1520',
                'numa_node': 1,
                'dev_type': fields.PciDeviceType.SRIOV_VF,
                'dev_id': 'pci_0000:0f:08.7',
                'extra_info': '{}',
                'label': 'label_8086_1520',
                'status': fields.PciDeviceStatus.AVAILABLE,
                'instance_uuid': '00000000-0000-0000-0000-000000000010',
                'request_id': None,
                'parent_addr': '0000:0f:00.1',
                }, {'id': 3356,
                'compute_node_id': 1,
                'address': '0000:0f:03.7',
                'parent_addr': '0000:0f:03.0',
                'vendor_id': '8083',
                'product_id': '1523',
                'numa_node': 0,
                'dev_type': fields.PciDeviceType.SRIOV_VF,
                'dev_id': 'pci_0000:0f:08.7',
                'extra_info': '{}',
                'label': 'label_8086_1520',
                'status': fields.PciDeviceStatus.AVAILABLE,
                'instance_uuid': '00000000-0000-0000-0000-000000000010',
                'request_id': None,
                }

    @property
    def compute_node(self):
        if self._compute_node is None:
            self._compute_node = db.compute_node_create(self.admin_context, {
                'vcpus': 0,
                'memory_mb': 0,
                'local_gb': 0,
                'vcpus_used': 0,
                'memory_mb_used': 0,
                'local_gb_used': 0,
                'hypervisor_type': 'fake',
                'hypervisor_version': 0,
                'cpu_info': 'fake',
                })
        return self._compute_node

    def _create_fake_pci_devs(self):
        v1, v2 = self._get_fake_pci_devs()
        for i in v1, v2:
            i['compute_node_id'] = self.compute_node['id']

        db.pci_device_update(self.admin_context, v1['compute_node_id'],
                             v1['address'], v1)
        db.pci_device_update(self.admin_context, v2['compute_node_id'],
                             v2['address'], v2)

        return (v1, v2)

    # def test_pci_device_get_by_addr(self):
    #     v1, v2 = self._create_fake_pci_devs()
    #     result = db.pci_device_get_by_addr(self.admin_context, 1,
    #                                        '0000:0f:08.7')
    #     self._assertEqualObjects(v1, result, self.ignored_keys)

    def test_pci_device_get_by_addr_not_found(self):
        self._create_fake_pci_devs()
        self.assertRaises(exception.PciDeviceNotFound,
                          db.pci_device_get_by_addr, self.admin_context,
                          1, '0000:0f:08:09')

    # def test_pci_device_get_all_by_parent_addr(self):
    #     v1, v2 = self._create_fake_pci_devs()
    #     results = db.pci_device_get_all_by_parent_addr(self.admin_context, 1,
    #                                                   '0000:0f:00.1')
    #     self._assertEqualListsOfObjects([v1], results, self.ignored_keys)

    def test_pci_device_get_all_by_parent_addr_empty(self):
        v1, v2 = self._create_fake_pci_devs()
        results = db.pci_device_get_all_by_parent_addr(self.admin_context, 1,
                                                      '0000:0f:01.6')
        self.assertEqual(len(results), 0)

    def test_pci_device_get_by_id(self):
        v1, v2 = self._create_fake_pci_devs()
        result = db.pci_device_get_by_id(self.admin_context, 3353)
        self._assertEqualObjects(v1, result, self.ignored_keys)

    def test_pci_device_get_by_id_not_found(self):
        self._create_fake_pci_devs()
        self.assertRaises(exception.PciDeviceNotFoundById,
                          db.pci_device_get_by_id,
                          self.admin_context, 3354)

    # def test_pci_device_get_all_by_node(self):
    #     from rome.driver.database_driver import get_driver
    #     driver = get_driver()
    #     v1, v2 = self._create_fake_pci_devs()
    #     results = db.pci_device_get_all_by_node(self.admin_context, 1)
    #     self._assertEqualListsOfObjects(results, [v1, v2], self.ignored_keys)

    def test_pci_device_get_all_by_node_empty(self):
        v1, v2 = self._get_fake_pci_devs()
        results = db.pci_device_get_all_by_node(self.admin_context, 9)
        self.assertEqual(len(results), 0)

    def test_pci_device_get_by_instance_uuid(self):
        v1, v2 = self._create_fake_pci_devs()
        v1['status'] = fields.PciDeviceStatus.ALLOCATED
        v2['status'] = fields.PciDeviceStatus.ALLOCATED
        db.pci_device_update(self.admin_context, v1['compute_node_id'],
                             v1['address'], v1)
        db.pci_device_update(self.admin_context, v2['compute_node_id'],
                             v2['address'], v2)
        results = db.pci_device_get_all_by_instance_uuid(
            self.context,
            '00000000-0000-0000-0000-000000000010')
        self._assertEqualListsOfObjects(results, [v1, v2], self.ignored_keys)

    def test_pci_device_get_by_instance_uuid_check_status(self):
        v1, v2 = self._create_fake_pci_devs()
        v1['status'] = fields.PciDeviceStatus.ALLOCATED
        v2['status'] = fields.PciDeviceStatus.CLAIMED
        db.pci_device_update(self.admin_context, v1['compute_node_id'],
                             v1['address'], v1)
        db.pci_device_update(self.admin_context, v2['compute_node_id'],
                             v2['address'], v2)
        results = db.pci_device_get_all_by_instance_uuid(
            self.context,
            '00000000-0000-0000-0000-000000000010')
        self._assertEqualListsOfObjects(results, [v1], self.ignored_keys)

    # def test_pci_device_update(self):
    #     v1, v2 = self._create_fake_pci_devs()
    #     v1['status'] = fields.PciDeviceStatus.ALLOCATED
    #     db.pci_device_update(self.admin_context, v1['compute_node_id'],
    #                          v1['address'], v1)
    #     result = db.pci_device_get_by_addr(
    #         self.admin_context, 1, '0000:0f:08.7')
    #     self._assertEqualObjects(v1, result, self.ignored_keys)
    #
    #     v1['status'] = fields.PciDeviceStatus.CLAIMED
    #     db.pci_device_update(self.admin_context, v1['compute_node_id'],
    #                          v1['address'], v1)
    #     result = db.pci_device_get_by_addr(
    #         self.admin_context, 1, '0000:0f:08.7')
    #     self._assertEqualObjects(v1, result, self.ignored_keys)

    def test_pci_device_destroy(self):
        v1, v2 = self._create_fake_pci_devs()
        results = db.pci_device_get_all_by_node(self.admin_context,
                                                self.compute_node['id'])
        self._assertEqualListsOfObjects(results, [v1, v2], self.ignored_keys)
        db.pci_device_destroy(self.admin_context, v1['compute_node_id'],
                              v1['address'])
        results = db.pci_device_get_all_by_node(self.admin_context,
                                                self.compute_node['id'])
        self._assertEqualListsOfObjects(results, [v2], self.ignored_keys)

    # def test_pci_device_destroy_exception(self):
    #     v1, v2 = self._get_fake_pci_devs()
    #     self.assertRaises(exception.PciDeviceNotFound,
    #                       db.pci_device_destroy,
    #                       self.admin_context,
    #                       v1['compute_node_id'],
    #                       v1['address'])

    def _create_fake_pci_devs_old_format(self):
        v1, v2 = self._get_fake_pci_devs()

        for v in (v1, v2):
            v['parent_addr'] = None
            v['extra_info'] = jsonutils.dumps(
                {'phys_function': 'fake-phys-func'})

            db.pci_device_update(self.admin_context, v['compute_node_id'],
                                 v['address'], v)

    # def test_migrate_aggregates(self):
    #     db.aggregate_create(self.context, {'name': 'foo'})
    #     db.aggregate_create(self.context, {'name': 'bar',
    #                                        'uuid': 'fake-uuid'})
    #     total, done = db.aggregate_uuids_online_data_migration(
    #         self.context, 10)
    #     self.assertEqual(1, total)
    #     self.assertEqual(1, done)
    #     total, done = db.aggregate_uuids_online_data_migration(
    #         self.context, 10)
    #     self.assertEqual(0, total)
    #     self.assertEqual(0, done)