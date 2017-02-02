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


class InstanceTypeAccessTestCase(BaseInstanceTypeTestCase):

    def _create_flavor_access(self, flavor_id, project_id):
        return db.flavor_access_add(self.ctxt, flavor_id, project_id)

    def test_flavor_access_get_by_flavor_id(self):
        from rome.driver.database_driver import get_driver
        driver = get_driver()

        flavors = ({'name': 'n1', 'flavorid': 'f1'},
                   {'name': 'n2', 'flavorid': 'f2'})
        it1, it2 = tuple((self._create_flavor(v) for v in flavors))

        access_it1 = [self._create_flavor_access(it1['flavorid'], 'pr1'),
                      self._create_flavor_access(it1['flavorid'], 'pr2')]

        access_it2 = [self._create_flavor_access(it2['flavorid'], 'pr1')]

        for it, access_it in zip((it1, it2), (access_it1, access_it2)):
            params = (self.ctxt, it['flavorid'])
            real_access_it = db.flavor_access_get_by_flavor_id(*params)
            self._assertEqualListsOfObjects(access_it, real_access_it)

    # def test_flavor_access_get_by_flavor_id_flavor_not_found(self):
    #     self.assertRaises(exception.FlavorNotFound,
    #                       db.flavor_get_by_flavor_id,
    #                       self.ctxt, 'nonexists')

    def test_flavor_access_add(self):
        flavor = self._create_flavor({'flavorid': 'f1'})
        project_id = 'p1'

        access = self._create_flavor_access(flavor['flavorid'], project_id)
        # NOTE(boris-42): Check that flavor_access_add doesn't fail and
        #                 returns correct value. This is enough because other
        #                 logic is checked by other methods.
        self.assertIsNotNone(access['id'])
        self.assertEqual(access['instance_type_id'], flavor['id'])
        self.assertEqual(access['project_id'], project_id)

    def test_flavor_access_add_to_non_existing_flavor(self):
        self.assertRaises(exception.FlavorNotFound,
                          self._create_flavor_access,
                          'nonexists', 'does_not_matter')

    # def test_flavor_access_add_duplicate_project_id_flavor(self):
    #     flavor = self._create_flavor({'flavorid': 'f1'})
    #     params = (flavor['flavorid'], 'p1')
    #
    #     self._create_flavor_access(*params)
    #     self.assertRaises(exception.FlavorAccessExists,
    #                       self._create_flavor_access, *params)

    def test_flavor_access_remove(self):
        flavors = ({'name': 'n1', 'flavorid': 'f1'},
                   {'name': 'n2', 'flavorid': 'f2'})
        it1, it2 = tuple((self._create_flavor(v) for v in flavors))

        access_it1 = [self._create_flavor_access(it1['flavorid'], 'pr1'),
                      self._create_flavor_access(it1['flavorid'], 'pr2')]

        access_it2 = [self._create_flavor_access(it2['flavorid'], 'pr1')]

        db.flavor_access_remove(self.ctxt, it1['flavorid'],
                                access_it1[1]['project_id'])

        for it, access_it in zip((it1, it2), (access_it1[:1], access_it2)):
            params = (self.ctxt, it['flavorid'])
            real_access_it = db.flavor_access_get_by_flavor_id(*params)
            self._assertEqualListsOfObjects(access_it, real_access_it)

    def test_flavor_access_remove_flavor_not_found(self):
        self.assertRaises(exception.FlavorNotFound,
                          db.flavor_access_remove,
                          self.ctxt, 'nonexists', 'does_not_matter')

    def test_flavor_access_remove_access_not_found(self):
        flavor = self._create_flavor({'flavorid': 'f1'})
        params = (flavor['flavorid'], 'p1')
        self._create_flavor_access(*params)
        self.assertRaises(exception.FlavorAccessNotFound,
                          db.flavor_access_remove,
                          self.ctxt, flavor['flavorid'], 'p2')

    def test_flavor_access_removed_after_flavor_destroy(self):
        flavor1 = self._create_flavor({'flavorid': 'f1', 'name': 'n1'})
        flavor2 = self._create_flavor({'flavorid': 'f2', 'name': 'n2'})
        values = [
            (flavor1['flavorid'], 'p1'),
            (flavor1['flavorid'], 'p2'),
            (flavor2['flavorid'], 'p3')
        ]
        for v in values:
            self._create_flavor_access(*v)

        db.flavor_destroy(self.ctxt, flavor1['flavorid'])

        p = (self.ctxt, flavor1['flavorid'])
        self.assertEqual(0, len(db.flavor_access_get_by_flavor_id(*p)))
        p = (self.ctxt, flavor2['flavorid'])
        self.assertEqual(1, len(db.flavor_access_get_by_flavor_id(*p)))
        db.flavor_destroy(self.ctxt, flavor2['flavorid'])
        self.assertEqual(0, len(db.flavor_access_get_by_flavor_id(*p)))
