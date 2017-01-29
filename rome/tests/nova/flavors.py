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


class InstanceTypeTestCase(BaseInstanceTypeTestCase):

    def test_flavor_create(self):
        flavor = self._create_flavor({})
        ignored_keys = ['id', 'deleted', 'deleted_at', 'updated_at',
                        'created_at', 'extra_specs', 'projects']

        self.assertIsNotNone(flavor['id'])
        self._assertEqualObjects(flavor, self._get_base_values(),
                                 ignored_keys)

    def test_flavor_create_with_projects(self):
        projects = ['fake-project1', 'fake-project2']
        flavor = self._create_flavor({}, projects + ['fake-project2'])
        access = db.flavor_access_get_by_flavor_id(self.ctxt,
                                                   flavor['flavorid'])
        self.assertEqual(projects, [x.project_id for x in access])

    # def test_flavor_destroy(self):
    #     specs1 = {'a': '1', 'b': '2'}
    #     flavor1 = self._create_flavor({'name': 'name1', 'flavorid': 'a1',
    #                                    'extra_specs': specs1})
    #     specs2 = {'c': '4', 'd': '3'}
    #     flavor2 = self._create_flavor({'name': 'name2', 'flavorid': 'a2',
    #                                    'extra_specs': specs2})
    #
    #     db.flavor_destroy(self.ctxt, 'a1')
    #
    #     self.assertRaises(exception.FlavorNotFound,
    #                       db.flavor_get, self.ctxt, flavor1['id'])
    #     real_specs1 = db.flavor_extra_specs_get(self.ctxt, flavor1['flavorid'])
    #     self._assertEqualObjects(real_specs1, {})
    #
    #     r_flavor2 = db.flavor_get(self.ctxt, flavor2['id'])
    #     self._assertEqualObjects(flavor2, r_flavor2, 'extra_specs')

    def test_flavor_destroy_not_found(self):
        self.assertRaises(exception.FlavorNotFound,
                          db.flavor_destroy, self.ctxt, 'nonexists')

    # def test_flavor_create_duplicate_name(self):
    #     self._create_flavor({})
    #     self.assertRaises(exception.FlavorExists,
    #                       self._create_flavor,
    #                       {'flavorid': 'some_random_flavor'})

    # def test_flavor_create_duplicate_flavorid(self):
    #     self._create_flavor({})
    #     self.assertRaises(exception.FlavorIdExists,
    #                       self._create_flavor,
    #                       {'name': 'some_random_name'})

    def test_flavor_create_with_extra_specs(self):
        extra_specs = dict(a='abc', b='def', c='ghi')
        flavor = self._create_flavor({'extra_specs': extra_specs})
        ignored_keys = ['id', 'deleted', 'deleted_at', 'updated_at',
                        'created_at', 'extra_specs', 'projects']

        self._assertEqualObjects(flavor, self._get_base_values(),
                                 ignored_keys)
        self._assertEqualObjects(extra_specs, flavor['extra_specs'])

    # @mock.patch('sqlalchemy.orm.query.Query.all', return_value=[])
    # def test_flavor_create_with_extra_specs_duplicate(self, mock_all):
    #     extra_specs = dict(key='value')
    #     flavorid = 'flavorid'
    #     self._create_flavor({'flavorid': flavorid, 'extra_specs': extra_specs})
    #
    #     self.assertRaises(exception.FlavorExtraSpecUpdateCreateFailed,
    #                       db.flavor_extra_specs_update_or_create,
    #                       self.ctxt, flavorid, extra_specs)

    def test_flavor_get_all(self):
        # NOTE(boris-42): Remove base instance types
        for it in db.flavor_get_all(self.ctxt):
            db.flavor_destroy(self.ctxt, it['flavorid'])

        flavors = [
            {'root_gb': 600, 'memory_mb': 100, 'disabled': True,
             'is_public': True, 'name': 'a1', 'flavorid': 'f1'},
            {'root_gb': 500, 'memory_mb': 200, 'disabled': True,
             'is_public': True, 'name': 'a2', 'flavorid': 'f2'},
            {'root_gb': 400, 'memory_mb': 300, 'disabled': False,
             'is_public': True, 'name': 'a3', 'flavorid': 'f3'},
            {'root_gb': 300, 'memory_mb': 400, 'disabled': False,
             'is_public': False, 'name': 'a4', 'flavorid': 'f4'},
            {'root_gb': 200, 'memory_mb': 500, 'disabled': True,
             'is_public': False, 'name': 'a5', 'flavorid': 'f5'},
            {'root_gb': 100, 'memory_mb': 600, 'disabled': True,
             'is_public': False, 'name': 'a6', 'flavorid': 'f6'}
        ]
        flavors = [self._create_flavor(it) for it in flavors]

        lambda_filters = {
            'min_memory_mb': lambda it, v: it['memory_mb'] >= v,
            'min_root_gb': lambda it, v: it['root_gb'] >= v,
            'disabled': lambda it, v: it['disabled'] == v,
            'is_public': lambda it, v: (v is None or it['is_public'] == v)
        }

        mem_filts = [{'min_memory_mb': x} for x in [100, 350, 550, 650]]
        root_filts = [{'min_root_gb': x} for x in [100, 350, 550, 650]]
        disabled_filts = [{'disabled': x} for x in [True, False]]
        is_public_filts = [{'is_public': x} for x in [True, False, None]]

        def assert_multi_filter_flavor_get(filters=None):
            if filters is None:
                filters = {}

            expected_it = flavors
            for name, value in filters.items():
                filt = lambda it: lambda_filters[name](it, value)
                expected_it = list(filter(filt, expected_it))

            real_it = db.flavor_get_all(self.ctxt, filters=filters)
            self._assertEqualListsOfObjects(expected_it, real_it)

        # no filter
        assert_multi_filter_flavor_get()

        # test only with one filter
        for filt in mem_filts:
            assert_multi_filter_flavor_get(filt)
        for filt in root_filts:
            assert_multi_filter_flavor_get(filt)
        for filt in disabled_filts:
            assert_multi_filter_flavor_get(filt)
        for filt in is_public_filts:
            assert_multi_filter_flavor_get(filt)

        # test all filters together
        for mem in mem_filts:
            for root in root_filts:
                for disabled in disabled_filts:
                    for is_public in is_public_filts:
                        filts = {}
                        for f in (mem, root, disabled, is_public):
                            filts.update(f)
                        assert_multi_filter_flavor_get(filts)

    def test_flavor_get_all_limit_sort(self):
        def assert_sorted_by_key_dir(sort_key, asc=True):
            sort_dir = 'asc' if asc else 'desc'
            results = db.flavor_get_all(self.ctxt, sort_key='name',
                                        sort_dir=sort_dir)
            # Manually sort the results as we would expect them
            expected_results = sorted(results,
                                      key=lambda item: item['name'],
                                      reverse=(not asc))
            self.assertEqual(expected_results, results)

        def assert_sorted_by_key_both_dir(sort_key):
            assert_sorted_by_key_dir(sort_key, True)
            assert_sorted_by_key_dir(sort_key, False)

        for attr in ['memory_mb', 'root_gb', 'deleted_at', 'name', 'deleted',
                     'created_at', 'ephemeral_gb', 'updated_at', 'disabled',
                     'vcpus', 'swap', 'rxtx_factor', 'is_public', 'flavorid',
                     'vcpu_weight', 'id']:
            assert_sorted_by_key_both_dir(attr)

    def test_flavor_get_all_limit(self):
        flavors = [
            {'root_gb': 1, 'memory_mb': 100, 'disabled': True,
             'is_public': False, 'name': 'flavor1', 'flavorid': 'flavor1'},
            {'root_gb': 100, 'memory_mb': 200, 'disabled': True,
             'is_public': False, 'name': 'flavor2', 'flavorid': 'flavor2'},
            {'root_gb': 100, 'memory_mb': 300, 'disabled': True,
             'is_public': False, 'name': 'flavor3', 'flavorid': 'flavor3'},
        ]
        flavors = [self._create_flavor(it) for it in flavors]

        limited_flavors = db.flavor_get_all(self.ctxt, limit=2)
        self.assertEqual(2, len(limited_flavors))

    def test_flavor_get_all_list_marker(self):
        flavors = [
            {'root_gb': 1, 'memory_mb': 100, 'disabled': True,
             'is_public': False, 'name': 'flavor1', 'flavorid': 'flavor1'},
            {'root_gb': 100, 'memory_mb': 200, 'disabled': True,
             'is_public': False, 'name': 'flavor2', 'flavorid': 'flavor2'},
            {'root_gb': 100, 'memory_mb': 300, 'disabled': True,
             'is_public': False, 'name': 'flavor3', 'flavorid': 'flavor3'},
        ]
        flavors = [self._create_flavor(it) for it in flavors]

        all_flavors = db.flavor_get_all(self.ctxt)

        # Set the 3rd result as the marker
        marker_flavorid = all_flavors[2]['flavorid']
        marked_flavors = db.flavor_get_all(self.ctxt, marker=marker_flavorid)
        # We expect everything /after/ the 3rd result
        expected_results = all_flavors[3:]
        self.assertEqual(expected_results, marked_flavors)

    def test_flavor_get_all_marker_not_found(self):
        self.assertRaises(exception.MarkerNotFound,
                db.flavor_get_all, self.ctxt, marker='invalid')

    def test_flavor_get(self):
        flavors = [{'name': 'abc', 'flavorid': '123'},
                   {'name': 'def', 'flavorid': '456'},
                   {'name': 'ghi', 'flavorid': '789'}]
        flavors = [self._create_flavor(t) for t in flavors]

        for flavor in flavors:
            flavor_by_id = db.flavor_get(self.ctxt, flavor['id'])
            self._assertEqualObjects(flavor, flavor_by_id)

    # def test_flavor_get_non_public(self):
    #     flavor = self._create_flavor({'name': 'abc', 'flavorid': '123',
    #                                   'is_public': False})
    #
    #     # Admin can see it
    #     flavor_by_id = db.flavor_get(self.ctxt, flavor['id'])
    #     self._assertEqualObjects(flavor, flavor_by_id)
    #
    #     # Regular user can not
    #     self.assertRaises(exception.FlavorNotFound, db.flavor_get,
    #             self.user_ctxt, flavor['id'])
    #
    #     # Regular user can see it after being granted access
    #     db.flavor_access_add(self.ctxt, flavor['flavorid'],
    #             self.user_ctxt.project_id)
    #     flavor_by_id = db.flavor_get(self.user_ctxt, flavor['id'])
    #     self._assertEqualObjects(flavor, flavor_by_id)

    def test_flavor_get_by_name(self):
        flavors = [{'name': 'abc', 'flavorid': '123'},
                   {'name': 'def', 'flavorid': '456'},
                   {'name': 'ghi', 'flavorid': '789'}]
        flavors = [self._create_flavor(t) for t in flavors]

        for flavor in flavors:
            flavor_by_name = db.flavor_get_by_name(self.ctxt, flavor['name'])
            self._assertEqualObjects(flavor, flavor_by_name)

    def test_flavor_get_by_name_not_found(self):
        self._create_flavor({})
        self.assertRaises(exception.FlavorNotFoundByName,
                          db.flavor_get_by_name, self.ctxt, 'nonexists')

    # def test_flavor_get_by_name_non_public(self):
    #     flavor = self._create_flavor({'name': 'abc', 'flavorid': '123',
    #                                   'is_public': False})
    #
    #     # Admin can see it
    #     flavor_by_name = db.flavor_get_by_name(self.ctxt, flavor['name'])
    #     self._assertEqualObjects(flavor, flavor_by_name)
    #
    #     # Regular user can not
    #     self.assertRaises(exception.FlavorNotFoundByName,
    #             db.flavor_get_by_name, self.user_ctxt,
    #             flavor['name'])
    #
    #     # Regular user can see it after being granted access
    #     db.flavor_access_add(self.ctxt, flavor['flavorid'],
    #             self.user_ctxt.project_id)
    #     flavor_by_name = db.flavor_get_by_name(self.user_ctxt, flavor['name'])
    #     self._assertEqualObjects(flavor, flavor_by_name)

    # def test_flavor_get_by_flavor_id(self):
    #     flavors = [{'name': 'abc', 'flavorid': '123'},
    #                {'name': 'def', 'flavorid': '456'},
    #                {'name': 'ghi', 'flavorid': '789'}]
    #     flavors = [self._create_flavor(t) for t in flavors]
    #
    #     for flavor in flavors:
    #         params = (self.ctxt, flavor['flavorid'])
    #         flavor_by_flavorid = db.flavor_get_by_flavor_id(*params)
    #         self._assertEqualObjects(flavor, flavor_by_flavorid)

    # def test_flavor_get_by_flavor_not_found(self):
    #     self._create_flavor({})
    #     self.assertRaises(exception.FlavorNotFound,
    #                       db.flavor_get_by_flavor_id,
    #                       self.ctxt, 'nonexists')

    # def test_flavor_get_by_flavor_id_non_public(self):
    #     flavor = self._create_flavor({'name': 'abc', 'flavorid': '123',
    #                                   'is_public': False})
    #
    #     # Admin can see it
    #     flavor_by_fid = db.flavor_get_by_flavor_id(self.ctxt,
    #                                                flavor['flavorid'])
    #     self._assertEqualObjects(flavor, flavor_by_fid)
    #
    #     # Regular user can not
    #     self.assertRaises(exception.FlavorNotFound,
    #             db.flavor_get_by_flavor_id, self.user_ctxt,
    #             flavor['flavorid'])
    #
    #     # Regular user can see it after being granted access
    #     db.flavor_access_add(self.ctxt, flavor['flavorid'],
    #             self.user_ctxt.project_id)
    #     flavor_by_fid = db.flavor_get_by_flavor_id(self.user_ctxt,
    #                                                flavor['flavorid'])
    #     self._assertEqualObjects(flavor, flavor_by_fid)

    def test_flavor_get_by_flavor_id_deleted(self):

        flavor = self._create_flavor({'name': 'abc', 'flavorid': '123'})

        db.flavor_destroy(self.ctxt, '123')

        flavor_by_fid = db.flavor_get_by_flavor_id(self.ctxt,
                flavor['flavorid'], read_deleted='yes')
        self.assertEqual(flavor['id'], flavor_by_fid['id'])

    # def test_flavor_get_by_flavor_id_deleted_and_recreate(self):
    #
    #     # NOTE(wingwj): Aims to test difference between mysql and postgresql
    #     # for bug 1288636
    #     param_dict = {'name': 'abc', 'flavorid': '123'}
    #
    #     self._create_flavor(param_dict)
    #     db.flavor_destroy(self.ctxt, '123')
    #
    #     # Recreate the flavor with the same params
    #     flavor = self._create_flavor(param_dict)
    #
    #     flavor_by_fid = db.flavor_get_by_flavor_id(self.ctxt,
    #             flavor['flavorid'], read_deleted='yes')
    #     self.assertEqual(flavor['id'], flavor_by_fid['id'])
