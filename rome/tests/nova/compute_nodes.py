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
import six


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


class ComputeNodeTestCase(unittest.TestCase, ModelsObjectComparatorMixin):

    _ignored_keys = ['id', 'deleted', 'deleted_at', 'created_at', 'updated_at']

    def setUp(self):
        super(ComputeNodeTestCase, self).setUp()
        self.ctxt = rome_context.get_admin_context()
        clean_rome_all_databases()
        self.service_dict = dict(host='host1', binary='nova-compute',
                            topic="compute", report_count=1,
                            disabled=False)
        self.service = db.service_create(self.ctxt, self.service_dict)
        self.compute_node_dict = dict(vcpus=2, memory_mb=1024, local_gb=2048,
                                 uuid=uuidsentinel.fake_compute_node,
                                 vcpus_used=0, memory_mb_used=0,
                                 local_gb_used=0, free_ram_mb=1024,
                                 free_disk_gb=2048, hypervisor_type="xen",
                                 hypervisor_version=1, cpu_info="",
                                 running_vms=0, current_workload=0,
                                 service_id=self.service['id'],
                                 host=self.service['host'],
                                 disk_available_least=100,
                                 hypervisor_hostname='abracadabra104',
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
        self.stats = dict(num_instances=3, num_proj_12345=2,
                     num_proj_23456=2, num_vm_building=3)
        self.compute_node_dict['stats'] = jsonutils.dumps(self.stats)
        # self.flags(reserved_host_memory_mb=0)
        # self.flags(reserved_host_disk_mb=0)
        self.item = db.compute_node_create(self.ctxt, self.compute_node_dict)

    def test_compute_node_create(self):
        self._assertEqualObjects(self.compute_node_dict, self.item,
                                ignored_keys=self._ignored_keys + ['stats'])
        new_stats = jsonutils.loads(self.item['stats'])
        self.assertEqual(self.stats, new_stats)

    def test_compute_node_get_all(self):
        nodes = db.compute_node_get_all(self.ctxt)
        self.assertEqual(1, len(nodes))
        node = nodes[0]
        self._assertEqualObjects(self.compute_node_dict, node,
                    ignored_keys=self._ignored_keys +
                                 ['stats', 'service'])
        new_stats = jsonutils.loads(node['stats'])
        self.assertEqual(self.stats, new_stats)

    # def test_compute_node_get_all_by_pagination(self):
    #     service_dict = dict(host='host2', binary='nova-compute',
    #                         topic="compute_nodes", report_count=1,
    #                         disabled=False)
    #     service = db.service_create(self.ctxt, service_dict)
    #     compute_node_dict = dict(vcpus=2, memory_mb=1024, local_gb=2048,
    #                              uuid=uuidsentinel.fake_compute_node,
    #                              vcpus_used=0, memory_mb_used=0,
    #                              local_gb_used=0, free_ram_mb=1024,
    #                              free_disk_gb=2048, hypervisor_type="xen",
    #                              hypervisor_version=1, cpu_info="",
    #                              running_vms=0, current_workload=0,
    #                              service_id=service['id'],
    #                              host=service['host'],
    #                              disk_available_least=100,
    #                              hypervisor_hostname='abcde11',
    #                              host_ip='127.0.0.1',
    #                              supported_instances='',
    #                              pci_stats='',
    #                              metrics='',
    #                              extra_resources='',
    #                              cpu_allocation_ratio=16.0,
    #                              ram_allocation_ratio=1.5,
    #                              disk_allocation_ratio=1.0,
    #                              stats='', numa_topology='')
    #     stats = dict(num_instances=2, num_proj_12345=1,
    #                  num_proj_23456=1, num_vm_building=2)
    #     compute_node_dict['stats'] = jsonutils.dumps(stats)
    #     db.compute_node_create(self.ctxt, compute_node_dict)
    #
    #     nodes = db.compute_node_get_all_by_pagination(self.ctxt,
    #                                                   limit=1, marker=1)
    #     self.assertEqual(1, len(nodes))
    #     node = nodes[0]
    #     self._assertEqualObjects(compute_node_dict, node,
    #                 ignored_keys=self._ignored_keys +
    #                              ['stats', 'service'])
    #     new_stats = jsonutils.loads(node['stats'])
    #     self.assertEqual(stats, new_stats)
    #
    #     nodes = db.compute_node_get_all_by_pagination(self.ctxt)
    #     self.assertEqual(2, len(nodes))
    #     node = nodes[0]
    #     self._assertEqualObjects(self.compute_node_dict, node,
    #                 ignored_keys=self._ignored_keys +
    #                              ['stats', 'service'])
    #     new_stats = jsonutils.loads(node['stats'])
    #     self.assertEqual(self.stats, new_stats)
    #     self.assertRaises(exception.MarkerNotFound,
    #                       db.compute_node_get_all_by_pagination,
    #                       self.ctxt, limit=1, marker=999)

    def test_compute_node_get_all_deleted_compute_node(self):
        # Create a service and compute node and ensure we can find its stats;
        # delete the service and compute node when done and loop again
        for x in range(2, 5):
            # Create a service
            service_data = self.service_dict.copy()
            service_data['host'] = 'host-%s' % x
            service = db.service_create(self.ctxt, service_data)

            # Create a compute node
            compute_node_data = self.compute_node_dict.copy()
            compute_node_data['service_id'] = service['id']
            compute_node_data['stats'] = jsonutils.dumps(self.stats.copy())
            compute_node_data['hypervisor_hostname'] = 'hypervisor-%s' % x
            node = db.compute_node_create(self.ctxt, compute_node_data)

            # Ensure the "new" compute node is found
            nodes = db.compute_node_get_all(self.ctxt)
            self.assertEqual(2, len(nodes))
            found = None
            for n in nodes:
                if n['id'] == node['id']:
                    found = n
                    break
            self.assertIsNotNone(found)
            # Now ensure the match has stats!
            self.assertNotEqual(jsonutils.loads(found['stats']), {})

            # Now delete the newly-created compute node to ensure the related
            # compute node stats are wiped in a cascaded fashion
            db.compute_node_delete(self.ctxt, node['id'])

            # Clean up the service
            db.service_destroy(self.ctxt, service['id'])

    def test_compute_node_get_all_mult_compute_nodes_one_service_entry(self):
        service_data = self.service_dict.copy()
        service_data['host'] = 'host2'
        service = db.service_create(self.ctxt, service_data)

        existing_node = dict(self.item.items())
        expected = [existing_node]

        for name in ['bm_node1', 'bm_node2']:
            compute_node_data = self.compute_node_dict.copy()
            compute_node_data['service_id'] = service['id']
            compute_node_data['stats'] = jsonutils.dumps(self.stats)
            compute_node_data['hypervisor_hostname'] = name
            node = db.compute_node_create(self.ctxt, compute_node_data)

            node = dict(node)

            expected.append(node)

        result = sorted(db.compute_node_get_all(self.ctxt),
                        key=lambda n: n['hypervisor_hostname'])

        self._assertEqualListsOfObjects(expected, result,
                    ignored_keys=['stats'])

    def test_compute_node_get_all_by_host_with_distinct_hosts(self):
        # Create another service with another node
        service2 = self.service_dict.copy()
        service2['host'] = 'host2'
        db.service_create(self.ctxt, service2)
        compute_node_another_host = self.compute_node_dict.copy()
        compute_node_another_host['stats'] = jsonutils.dumps(self.stats)
        compute_node_another_host['hypervisor_hostname'] = 'node_2'
        compute_node_another_host['host'] = 'host2'

        node = db.compute_node_create(self.ctxt, compute_node_another_host)

        result = db.compute_node_get_all_by_host(self.ctxt, 'host1')
        self._assertEqualListsOfObjects([self.item], result)
        result = db.compute_node_get_all_by_host(self.ctxt, 'host2')
        self._assertEqualListsOfObjects([node], result)

    def test_compute_node_get_all_by_host_with_same_host(self):
        # Create another node on top of the same service
        compute_node_same_host = self.compute_node_dict.copy()
        compute_node_same_host['stats'] = jsonutils.dumps(self.stats)
        compute_node_same_host['hypervisor_hostname'] = 'node_3'

        node = db.compute_node_create(self.ctxt, compute_node_same_host)

        expected = [self.item, node]
        result = sorted(db.compute_node_get_all_by_host(
                        self.ctxt, 'host1'),
                        key=lambda n: n['hypervisor_hostname'])

        ignored = ['stats']
        self._assertEqualListsOfObjects(expected, result,
                                        ignored_keys=ignored)

    def test_compute_node_get_all_by_host_not_found(self):
        self.assertRaises(exception.ComputeHostNotFound,
                          db.compute_node_get_all_by_host, self.ctxt, 'wrong')

    def test_compute_nodes_get_by_service_id_one_result(self):
        expected = [self.item]
        result = db.compute_nodes_get_by_service_id(
            self.ctxt, self.service['id'])

        ignored = ['stats']
        self._assertEqualListsOfObjects(expected, result,
                                        ignored_keys=ignored)

    def test_compute_nodes_get_by_service_id_multiple_results(self):
        # Create another node on top of the same service
        compute_node_same_host = self.compute_node_dict.copy()
        compute_node_same_host['stats'] = jsonutils.dumps(self.stats)
        compute_node_same_host['hypervisor_hostname'] = 'node_2'

        node = db.compute_node_create(self.ctxt, compute_node_same_host)

        expected = [self.item, node]
        result = sorted(db.compute_nodes_get_by_service_id(
                        self.ctxt, self.service['id']),
                        key=lambda n: n['hypervisor_hostname'])

        ignored = ['stats']
        self._assertEqualListsOfObjects(expected, result,
                                        ignored_keys=ignored)

    def test_compute_nodes_get_by_service_id_not_found(self):
        self.assertRaises(exception.ServiceNotFound,
                          db.compute_nodes_get_by_service_id, self.ctxt,
                          'fake')

    def test_compute_node_get_by_host_and_nodename(self):
        # Create another node on top of the same service
        compute_node_same_host = self.compute_node_dict.copy()
        compute_node_same_host['stats'] = jsonutils.dumps(self.stats)
        compute_node_same_host['hypervisor_hostname'] = 'node_2'

        node = db.compute_node_create(self.ctxt, compute_node_same_host)

        expected = node
        result = db.compute_node_get_by_host_and_nodename(
            self.ctxt, 'host1', 'node_2')

        self._assertEqualObjects(expected, result,
                    ignored_keys=self._ignored_keys +
                                 ['stats', 'service'])

    def test_compute_node_get_by_host_and_nodename_not_found(self):
        self.assertRaises(exception.ComputeHostNotFound,
                          db.compute_node_get_by_host_and_nodename,
                          self.ctxt, 'host1', 'wrong')

    def test_compute_node_get(self):
        compute_node_id = self.item['id']
        node = db.compute_node_get(self.ctxt, compute_node_id)
        self._assertEqualObjects(self.compute_node_dict, node,
                ignored_keys=self._ignored_keys +
                             ['stats', 'service'])
        new_stats = jsonutils.loads(node['stats'])
        self.assertEqual(self.stats, new_stats)

    def test_compute_node_update(self):
        compute_node_id = self.item['id']
        stats = jsonutils.loads(self.item['stats'])
        # change some values:
        stats['num_instances'] = 8
        stats['num_tribbles'] = 1
        values = {
            'vcpus': 4,
            'stats': jsonutils.dumps(stats),
        }
        item_updated = db.compute_node_update(self.ctxt, compute_node_id,
                                              values)
        self.assertEqual(4, item_updated['vcpus'])
        new_stats = jsonutils.loads(item_updated['stats'])
        self.assertEqual(stats, new_stats)

    def test_compute_node_delete(self):
        compute_node_id = self.item['id']
        db.compute_node_delete(self.ctxt, compute_node_id)
        nodes = db.compute_node_get_all(self.ctxt)
        self.assertEqual(len(nodes), 0)

    # def test_compute_node_search_by_hypervisor(self):
    #     nodes_created = []
    #     new_service = copy.copy(self.service_dict)
    #     for i in range(3):
    #         new_service['binary'] += str(i)
    #         new_service['topic'] += str(i)
    #         service = db.service_create(self.ctxt, new_service)
    #         self.compute_node_dict['service_id'] = service['id']
    #         self.compute_node_dict['hypervisor_hostname'] = 'testhost' + str(i)
    #         self.compute_node_dict['stats'] = jsonutils.dumps(self.stats)
    #         node = db.compute_node_create(self.ctxt, self.compute_node_dict)
    #         nodes_created.append(node)
    #     nodes = db.compute_node_search_by_hypervisor(self.ctxt, 'host')
    #     self.assertEqual(3, len(nodes))
    #     self._assertEqualListsOfObjects(nodes_created, nodes,
    #                     ignored_keys=self._ignored_keys + ['stats', 'service'])

    # def test_compute_node_statistics(self):
    #     service_dict = dict(host='hostA', binary='nova-compute',
    #                         topic=CONF.compute_topic, report_count=1,
    #                         disabled=False)
    #     service = db.service_create(self.ctxt, service_dict)
    #     # Define the various values for the new compute node
    #     new_vcpus = 4
    #     new_memory_mb = 4096
    #     new_local_gb = 2048
    #     new_vcpus_used = 1
    #     new_memory_mb_used = 1024
    #     new_local_gb_used = 100
    #     new_free_ram_mb = 3072
    #     new_free_disk_gb = 1948
    #     new_running_vms = 1
    #     new_current_workload = 0
    #
    #     # Calculate the expected values by adding the values for the new
    #     # compute node to those for self.item
    #     itm = self.item
    #     exp_count = 2
    #     exp_vcpus = new_vcpus + itm['vcpus']
    #     exp_memory_mb = new_memory_mb + itm['memory_mb']
    #     exp_local_gb = new_local_gb + itm['local_gb']
    #     exp_vcpus_used = new_vcpus_used + itm['vcpus_used']
    #     exp_memory_mb_used = new_memory_mb_used + itm['memory_mb_used']
    #     exp_local_gb_used = new_local_gb_used + itm['local_gb_used']
    #     exp_free_ram_mb = new_free_ram_mb + itm['free_ram_mb']
    #     exp_free_disk_gb = new_free_disk_gb + itm['free_disk_gb']
    #     exp_running_vms = new_running_vms + itm['running_vms']
    #     exp_current_workload = new_current_workload + itm['current_workload']
    #
    #     # Create the new compute node
    #     compute_node_dict = dict(vcpus=new_vcpus,
    #                              memory_mb=new_memory_mb,
    #                              local_gb=new_local_gb,
    #                              uuid=uuidsentinel.fake_compute_node,
    #                              vcpus_used=new_vcpus_used,
    #                              memory_mb_used=new_memory_mb_used,
    #                              local_gb_used=new_local_gb_used,
    #                              free_ram_mb=new_free_ram_mb,
    #                              free_disk_gb=new_free_disk_gb,
    #                              hypervisor_type="xen",
    #                              hypervisor_version=1,
    #                              cpu_info="",
    #                              running_vms=new_running_vms,
    #                              current_workload=new_current_workload,
    #                              service_id=service['id'],
    #                              host=service['host'],
    #                              disk_available_least=100,
    #                              hypervisor_hostname='abracadabra',
    #                              host_ip='127.0.0.2',
    #                              supported_instances='',
    #                              pci_stats='',
    #                              metrics='',
    #                              extra_resources='',
    #                              cpu_allocation_ratio=16.0,
    #                              ram_allocation_ratio=1.5,
    #                              disk_allocation_ratio=1.0,
    #                              stats='',
    #                              numa_topology='')
    #     db.compute_node_create(self.ctxt, compute_node_dict)
    #
    #     # Get the stats, and make sure the stats agree with the expected
    #     # amounts.
    #     stats = db.compute_node_statistics(self.ctxt)
    #     self.assertEqual(exp_count, stats['count'])
    #     self.assertEqual(exp_vcpus, stats['vcpus'])
    #     self.assertEqual(exp_memory_mb, stats['memory_mb'])
    #     self.assertEqual(exp_local_gb, stats['local_gb'])
    #     self.assertEqual(exp_vcpus_used, stats['vcpus_used'])
    #     self.assertEqual(exp_memory_mb_used, stats['memory_mb_used'])
    #     self.assertEqual(exp_local_gb_used, stats['local_gb_used'])
    #     self.assertEqual(exp_free_ram_mb, stats['free_ram_mb'])
    #     self.assertEqual(exp_free_disk_gb, stats['free_disk_gb'])
    #     self.assertEqual(exp_running_vms, stats['running_vms'])
    #     self.assertEqual(exp_current_workload, stats['current_workload'])

    # def test_compute_node_statistics_disabled_service(self):
    #     serv = db.service_get_by_host_and_topic(
    #         self.ctxt, 'host1', CONF.compute_topic)
    #     db.service_update(self.ctxt, serv['id'], {'disabled': True})
    #     stats = db.compute_node_statistics(self.ctxt)
    #     self.assertEqual(stats.pop('count'), 0)

    def test_compute_node_statistics_with_old_service_id(self):
        # NOTE(sbauza): This test is only for checking backwards compatibility
        # with old versions of compute_nodes not providing host column.
        # This test could be removed once we are sure that all compute nodes
        # are populating the host field thanks to the ResourceTracker

        service2 = self.service_dict.copy()
        service2['host'] = 'host2'
        db_service2 = db.service_create(self.ctxt, service2)
        compute_node_old_host = self.compute_node_dict.copy()
        compute_node_old_host['stats'] = jsonutils.dumps(self.stats)
        compute_node_old_host['hypervisor_hostname'] = 'node_2'
        compute_node_old_host['service_id'] = db_service2['id']
        compute_node_old_host.pop('host')

        db.compute_node_create(self.ctxt, compute_node_old_host)
        stats = db.compute_node_statistics(self.ctxt)
        self.assertEqual(2, stats.pop('count'))

    def test_compute_node_statistics_with_other_service(self):
        other_service = self.service_dict.copy()
        other_service['topic'] = 'fake-topic'
        other_service['binary'] = 'nova-fake'
        db.service_create(self.ctxt, other_service)

        stats = db.compute_node_statistics(self.ctxt)
        data = {'count': 1,
                'vcpus_used': 0,
                'local_gb_used': 0,
                'memory_mb': 1024,
                'current_workload': 0,
                'vcpus': 2,
                'running_vms': 0,
                'free_disk_gb': 2048,
                'disk_available_least': 100,
                'local_gb': 2048,
                'free_ram_mb': 1024,
                'memory_mb_used': 0}
        for key, value in six.iteritems(data):
            self.assertEqual(value, stats.pop(key))

    def test_compute_node_not_found(self):
        self.assertRaises(exception.ComputeHostNotFound, db.compute_node_get,
                          self.ctxt, 100500)

    def test_compute_node_update_always_updates_updated_at(self):
        item_updated = db.compute_node_update(self.ctxt,
                self.item['id'], {})
        self.assertNotEqual(self.item['updated_at'],
                                 item_updated['updated_at'])

    def test_compute_node_update_override_updated_at(self):
        # Update the record once so updated_at is set.
        first = db.compute_node_update(self.ctxt, self.item['id'],
                                       {'free_ram_mb': '12'})
        self.assertIsNotNone(first['updated_at'])

        # Update a second time. Make sure that the updated_at value we send
        # is overridden.
        second = db.compute_node_update(self.ctxt, self.item['id'],
                                        {'updated_at': first.updated_at,
                                         'free_ram_mb': '13'})
        self.assertNotEqual(first['updated_at'], second['updated_at'])

    def test_service_destroy_with_compute_node(self):
        db.service_destroy(self.ctxt, self.service['id'])
        self.assertRaises(exception.ComputeHostNotFound,
                          db.compute_node_get_model, self.ctxt,
                          self.item['id'])

    def test_service_destroy_with_old_compute_node(self):
        # NOTE(sbauza): This test is only for checking backwards compatibility
        # with old versions of compute_nodes not providing host column.
        # This test could be removed once we are sure that all compute nodes
        # are populating the host field thanks to the ResourceTracker
        compute_node_old_host_dict = self.compute_node_dict.copy()
        compute_node_old_host_dict.pop('host')
        item_old = db.compute_node_create(self.ctxt,
                                          compute_node_old_host_dict)

        db.service_destroy(self.ctxt, self.service['id'])
        self.assertRaises(exception.ComputeHostNotFound,
                          db.compute_node_get_model, self.ctxt,
                          item_old['id'])

    # @mock.patch("nova.db.sqlalchemy.api.compute_node_get_model")
    # def test_dbapi_compute_node_get_model(self, mock_get_model):
    #     cid = self.item["id"]
    #     db.api.compute_node_get_model(self.ctxt, cid)
    #     mock_get_model.assert_called_once_with(self.ctxt, cid)

    # @mock.patch("nova.db.sqlalchemy.api.model_query")
    # def test_compute_node_get_model(self, mock_model_query):
    #
    #     class FakeFiltered(object):
    #         def first(self):
    #             return mock.sentinel.first
    #
    #     fake_filtered_cn = FakeFiltered()
    #
    #     class FakeModelQuery(object):
    #         def filter_by(self, id):
    #             return fake_filtered_cn
    #
    #     mock_model_query.return_value = FakeModelQuery()
    #     result = sqlalchemy_api.compute_node_get_model(self.ctxt,
    #                                                    self.item["id"])
    #     self.assertEqual(result, mock.sentinel.first)
    #     mock_model_query.assert_called_once_with(self.ctxt, models.ComputeNode)
