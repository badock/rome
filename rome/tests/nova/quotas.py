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
from nova import quota
import api as sqlalchemy_api


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


def _reservation_get(context, uuid):
    @sqlalchemy_api.pick_context_manager_reader
    def doit(context):
        from models import Reservation
        return sqlalchemy_api.model_query(
            context, Reservation, read_deleted="no").filter_by(
            uuid=uuid).first()

    result = doit(context)
    if not result:
        raise exception.ReservationNotFound(uuid=uuid)

    return result


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


def _quota_reserve(context, project_id, user_id):
    """Create sample Quota, QuotaUsage and Reservation objects.

    There is no method db.quota_usage_create(), so we have to use
    db.quota_reserve() for creating QuotaUsage objects.

    Returns reservations uuids.

    """
    def get_sync(resource, usage):
        def sync(elevated, project_id, user_id):
            return {resource: usage}
        return sync
    quotas = {}
    user_quotas = {}
    resources = {}
    deltas = {}
    for i in range(3):
        resource = 'resource%d' % i
        if i == 2:
            # test for project level resources
            resource = 'fixed_ips'
            quotas[resource] = db.quota_create(context,
                                               project_id,
                                               resource, i + 2).hard_limit
            user_quotas[resource] = quotas[resource]
        else:
            quotas[resource] = db.quota_create(context,
                                               project_id,
                                               resource, i + 1).hard_limit
            user_quotas[resource] = db.quota_create(context, project_id,
                                                    resource, i + 1,
                                                    user_id=user_id).hard_limit
        sync_name = '_sync_%s' % resource
        resources[resource] = quota.ReservableResource(
            resource, sync_name, 'quota_res_%d' % i)
        deltas[resource] = i
        setattr(sqlalchemy_api, sync_name, get_sync(resource, i))
        sqlalchemy_api.QUOTA_SYNC_FUNCTIONS[sync_name] = getattr(
            sqlalchemy_api, sync_name)
    return db.quota_reserve(context, resources, quotas, user_quotas, deltas,
                    timeutils.utcnow(), 1000,
                    datetime.timedelta(days=1), project_id, user_id)



class QuotaTestCase(unittest.TestCase, ModelsObjectComparatorMixin):

    """Tests for db.api.quota_* methods."""

    def setUp(self):
        super(QuotaTestCase, self).setUp()
        self.ctxt = rome_context.get_admin_context()
        clean_rome_all_databases()

    def test_quota_create(self):
        quota = db.quota_create(self.ctxt, 'project1', 'resource', 99)
        self.assertEqual(quota.resource, 'resource')
        self.assertEqual(quota.hard_limit, 99)
        self.assertEqual(quota.project_id, 'project1')

    def test_quota_get(self):
        quota = db.quota_create(self.ctxt, 'project1', 'resource', 99)
        quota_db = db.quota_get(self.ctxt, 'project1', 'resource')
        self._assertEqualObjects(quota, quota_db)

    def test_quota_get_all_by_project(self):
        for i in range(3):
            for j in range(3):
                db.quota_create(self.ctxt, 'proj%d' % i, 'resource%d' % j, j)
        for i in range(3):
            quotas_db = db.quota_get_all_by_project(self.ctxt, 'proj%d' % i)
            self.assertEqual(quotas_db, {'project_id': 'proj%d' % i,
                                                        'resource0': 0,
                                                        'resource1': 1,
                                                        'resource2': 2})

    def test_quota_get_all_by_project_and_user(self):
        for i in range(3):
            for j in range(3):
                db.quota_create(self.ctxt, 'proj%d' % i, 'resource%d' % j,
                                j - 1, user_id='user%d' % i)
        for i in range(3):
            quotas_db = db.quota_get_all_by_project_and_user(self.ctxt,
                                                             'proj%d' % i,
                                                             'user%d' % i)
            self.assertEqual(quotas_db, {'project_id': 'proj%d' % i,
                                         'user_id': 'user%d' % i,
                                                        'resource0': -1,
                                                        'resource1': 0,
                                                        'resource2': 1})

    def test_quota_update(self):
        db.quota_create(self.ctxt, 'project1', 'resource1', 41)
        db.quota_update(self.ctxt, 'project1', 'resource1', 42)
        quota = db.quota_get(self.ctxt, 'project1', 'resource1')
        self.assertEqual(quota.hard_limit, 42)
        self.assertEqual(quota.resource, 'resource1')
        self.assertEqual(quota.project_id, 'project1')

    def test_quota_update_nonexistent(self):
        self.assertRaises(exception.ProjectQuotaNotFound,
            db.quota_update, self.ctxt, 'project1', 'resource1', 42)

    def test_quota_get_nonexistent(self):
        self.assertRaises(exception.ProjectQuotaNotFound,
            db.quota_get, self.ctxt, 'project1', 'resource1')

    def test_quota_reserve_all_resources(self):
        quotas = {}
        deltas = {}
        reservable_resources = {}
        for i, resource in enumerate(quota.resources):
            if isinstance(resource, quota.ReservableResource):
                quotas[resource.name] = db.quota_create(self.ctxt, 'project1',
                                                        resource.name,
                                                        100).hard_limit
                deltas[resource.name] = i
                reservable_resources[resource.name] = resource

        usages = {'instances': 3, 'cores': 6, 'ram': 9}
        instances = []
        for i in range(3):
            instances.append(db.instance_create(self.ctxt,
                             {'vcpus': 2, 'memory_mb': 3,
                             'project_id': 'project1'}))

        usages['fixed_ips'] = 2
        network = db.network_create_safe(self.ctxt, {})
        for i in range(2):
            address = '192.168.0.%d' % i
            db.fixed_ip_create(self.ctxt, {'project_id': 'project1',
                                           'address': address,
                                           'network_id': network['id']})
            db.fixed_ip_associate(self.ctxt, address,
                                  instances[0].uuid, network['id'])

        usages['floating_ips'] = 5
        for i in range(5):
            db.floating_ip_create(self.ctxt, {'project_id': 'project1'})

        usages['security_groups'] = 3
        for i in range(3):
            db.security_group_create(self.ctxt, {'project_id': 'project1'})

        usages['server_groups'] = 4
        for i in range(4):
            db.instance_group_create(self.ctxt, {'uuid': str(i),
                                                 'project_id': 'project1'})

        reservations_uuids = db.quota_reserve(self.ctxt, reservable_resources,
                                              quotas, quotas, deltas, None,
                                              None, None, 'project1')
        resources_names = list(reservable_resources.keys())
        for reservation_uuid in reservations_uuids:
            reservation = _reservation_get(self.ctxt, reservation_uuid)
            usage = db.quota_usage_get(self.ctxt, 'project1',
                                       reservation.resource)
            self.assertEqual(usage.in_use, usages[reservation.resource],
                             'Resource: %s' % reservation.resource)
            self.assertEqual(usage.reserved, deltas[reservation.resource])
            self.assertIn(reservation.resource, resources_names)
            resources_names.remove(reservation.resource)
        self.assertEqual(len(resources_names), 0)

    def test_quota_destroy_all_by_project(self):
        reservations = _quota_reserve(self.ctxt, 'project1', 'user1')
        db.quota_destroy_all_by_project(self.ctxt, 'project1')
        self.assertEqual(db.quota_get_all_by_project(self.ctxt, 'project1'),
                            {'project_id': 'project1'})
        self.assertEqual(db.quota_get_all_by_project_and_user(self.ctxt,
                            'project1', 'user1'),
                            {'project_id': 'project1', 'user_id': 'user1'})
        self.assertEqual(db.quota_usage_get_all_by_project(
                            self.ctxt, 'project1'),
                            {'project_id': 'project1'})
        for r in reservations:
            self.assertRaises(exception.ReservationNotFound,
                            _reservation_get, self.ctxt, r)

    def test_quota_destroy_all_by_project_and_user(self):
        reservations = _quota_reserve(self.ctxt, 'project1', 'user1')
        db.quota_destroy_all_by_project_and_user(self.ctxt, 'project1',
                                                 'user1')
        self.assertEqual(db.quota_get_all_by_project_and_user(self.ctxt,
                            'project1', 'user1'),
                            {'project_id': 'project1',
                             'user_id': 'user1'})
        self.assertEqual(db.quota_usage_get_all_by_project_and_user(
                            self.ctxt, 'project1', 'user1'),
                            {'project_id': 'project1',
                             'user_id': 'user1',
                             'fixed_ips': {'in_use': 2, 'reserved': 2}})
        for r in reservations:
            self.assertRaises(exception.ReservationNotFound,
                            _reservation_get, self.ctxt, r)

    def test_quota_usage_get_nonexistent(self):
        self.assertRaises(exception.QuotaUsageNotFound, db.quota_usage_get,
            self.ctxt, 'p1', 'nonexitent_resource')

    def test_quota_usage_get(self):
        _quota_reserve(self.ctxt, 'p1', 'u1')
        quota_usage = db.quota_usage_get(self.ctxt, 'p1', 'resource0')
        expected = {'resource': 'resource0', 'project_id': 'p1',
                    'in_use': 0, 'reserved': 0, 'total': 0}
        for key, value in expected.items():
            self.assertEqual(value, quota_usage[key])

    def test_quota_usage_get_all_by_project(self):
        _quota_reserve(self.ctxt, 'p1', 'u1')
        expected = {'project_id': 'p1',
                    'resource0': {'in_use': 0, 'reserved': 0},
                    'resource1': {'in_use': 1, 'reserved': 1},
                    'fixed_ips': {'in_use': 2, 'reserved': 2}}
        self.assertEqual(expected, db.quota_usage_get_all_by_project(
                         self.ctxt, 'p1'))

    def test_quota_usage_get_all_by_project_and_user(self):
        _quota_reserve(self.ctxt, 'p1', 'u1')
        expected = {'project_id': 'p1',
                    'user_id': 'u1',
                    'resource0': {'in_use': 0, 'reserved': 0},
                    'resource1': {'in_use': 1, 'reserved': 1},
                    'fixed_ips': {'in_use': 2, 'reserved': 2}}
        self.assertEqual(expected, db.quota_usage_get_all_by_project_and_user(
                         self.ctxt, 'p1', 'u1'))

    # def test_get_project_user_quota_usages_in_order(self):
    #     _quota_reserve(self.ctxt, 'p1', 'u1')
    #
    #     # @sqlalchemy_api.pick_context_manager_reader
    #     # def test(context):
    #     #     with mock.patch.object(query.Query, 'order_by') as order_mock:
    #     #         sqlalchemy_api._get_project_user_quota_usages(
    #     #             context, 'p1', 'u1')
    #     #         self.assertTrue(order_mock.called)
    #
    #     test(self.ctxt)

    def test_quota_usage_update_nonexistent(self):
        self.assertRaises(exception.QuotaUsageNotFound, db.quota_usage_update,
            self.ctxt, 'p1', 'u1', 'resource', in_use=42)

    def test_quota_usage_update(self):
        _quota_reserve(self.ctxt, 'p1', 'u1')
        db.quota_usage_update(self.ctxt, 'p1', 'u1', 'resource0', in_use=42,
                              reserved=43)
        quota_usage = db.quota_usage_get(self.ctxt, 'p1', 'resource0', 'u1')
        expected = {'resource': 'resource0', 'project_id': 'p1',
                    'user_id': 'u1', 'in_use': 42, 'reserved': 43, 'total': 85}
        for key, value in expected.items():
            self.assertEqual(value, quota_usage[key])

    def test_quota_create_exists(self):
        db.quota_create(self.ctxt, 'project1', 'resource1', 41)
        self.assertRaises(exception.QuotaExists, db.quota_create, self.ctxt,
                          'project1', 'resource1', 42)
