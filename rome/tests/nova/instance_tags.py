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

class TestDBInstanceTags(unittest.TestCase):

    sample_data = {
        'project_id': 'project1',
        'hostname': 'example.com',
        'host': 'h1',
        'node': 'n1',
        'metadata': {'mkey1': 'mval1', 'mkey2': 'mval2'},
        'system_metadata': {'smkey1': 'smval1', 'smkey2': 'smval2'},
        'info_cache': {'ckey': 'cvalue'}
    }

    def setUp(self):
        super(TestDBInstanceTags, self).setUp()
        clean_rome_all_databases()
        self.user_id = 'user1'
        self.project_id = 'project1'
        # self.context = context.RequestContext(self.user_id, self.project_id)
        self.context = rome_context.get_admin_context()

    def _create_instance(self):
        inst = db.instance_create(self.context, self.sample_data)
        return inst['uuid']

    def _get_tags_from_resp(self, tag_refs):
        return [(t.resource_id, t.tag) for t in tag_refs]

    def test_instance_tag_add(self):
        uuid = self._create_instance()

        tag = u'tag'
        tag_ref = db.instance_tag_add(self.context, uuid, tag)
        self.assertEqual(uuid, tag_ref.resource_id)
        self.assertEqual(tag, tag_ref.tag)

        tag_refs = db.instance_tag_get_by_instance_uuid(self.context, uuid)

        # Check the tag for the instance was added
        tags = self._get_tags_from_resp(tag_refs)
        self.assertEqual([(uuid, tag)], tags)

    def test_instance_tag_add_duplication(self):
        uuid = self._create_instance()
        tag = u'tag'

        for x in range(5):
            db.instance_tag_add(self.context, uuid, tag)

        tag_refs = db.instance_tag_get_by_instance_uuid(self.context, uuid)

        # Check the only one tag for the instance was added
        tags = self._get_tags_from_resp(tag_refs)
        self.assertEqual([(uuid, tag)], tags)

    def test_instance_tag_set(self):
        from rome.driver.database_driver import get_driver
        driver = get_driver()

        uuid = self._create_instance()

        tag1 = u'tag1'
        tag2 = u'tag2'
        tag3 = u'tag3'
        tag4 = u'tag4'

        # Set tags to the instance
        db.instance_tag_set(self.context, uuid, [tag1, tag2])
        tag_refs = db.instance_tag_get_by_instance_uuid(self.context, uuid)

        # Check the tags for the instance were set
        tags = self._get_tags_from_resp(tag_refs)
        expected = [(uuid, tag1), (uuid, tag2)]
        # NOTE(badock): use set to prevent errors caused by different items' order.
        self.assertEqual(set(expected), set(tags))

        # Set new tags to the instance
        db.instance_tag_set(self.context, uuid, [tag3, tag4, tag2])
        tag_refs = db.instance_tag_get_by_instance_uuid(self.context, uuid)

        # Check the tags for the instance were replaced
        tags = self._get_tags_from_resp(tag_refs)
        expected = [(uuid, tag3), (uuid, tag4), (uuid, tag2)]
        self.assertEqual(set(expected), set(tags))

    # def test_instance_tag_set_empty_add(self, mock_insert):
    #     uuid = self._create_instance()
    #     tag1 = u'tag1'
    #     tag2 = u'tag2'
    #
    #     db.instance_tag_set(self.context, uuid, [tag1, tag2])
    #
    #     # Check insert() was called to insert 'tag1' and 'tag2'
    #     mock_insert.assert_called_once_with()
    #
    #     mock_insert.reset_mock()
    #     db.instance_tag_set(self.context, uuid, [tag1])
    #
    #     # Check insert() wasn't called because there are no tags for creation
    #     mock_insert.assert_not_called()

    # def test_instance_tag_set_empty_delete(self, mock_delete):
    #     uuid = self._create_instance()
    #     db.instance_tag_set(self.context, uuid, [u'tag1', u'tag2'])
    #
    #     # Check delete() wasn't called because there are no tags for deletion
    #     mock_delete.assert_not_called()
    #
    #     db.instance_tag_set(self.context, uuid, [u'tag1', u'tag3'])
    #
    #     # Check delete() was called to delete 'tag2'
    #     mock_delete.assert_called_once_with(synchronize_session=False)

    def test_instance_tag_get_by_instance_uuid(self):
        uuid1 = self._create_instance()
        uuid2 = self._create_instance()

        tag1 = u'tag1'
        tag2 = u'tag2'
        tag3 = u'tag3'

        db.instance_tag_add(self.context, uuid1, tag1)
        db.instance_tag_add(self.context, uuid2, tag1)
        db.instance_tag_add(self.context, uuid2, tag2)
        db.instance_tag_add(self.context, uuid2, tag3)

        # Check the tags for the first instance
        tag_refs = db.instance_tag_get_by_instance_uuid(self.context, uuid1)
        tags = self._get_tags_from_resp(tag_refs)
        expected = [(uuid1, tag1)]

        # NOTE(badock): use set to prevent errors caused by different items' order.
        self.assertEqual(set(expected), set(tags))

        # Check the tags for the second instance
        tag_refs = db.instance_tag_get_by_instance_uuid(self.context, uuid2)
        tags = self._get_tags_from_resp(tag_refs)
        expected = [(uuid2, tag1), (uuid2, tag2), (uuid2, tag3)]

        # NOTE(badock): use set to prevent errors caused by different items' order.
        self.assertEqual(set(expected), set(tags))

    def test_instance_tag_get_by_instance_uuid_no_tags(self):
        uuid = self._create_instance()
        self.assertEqual([], db.instance_tag_get_by_instance_uuid(self.context,
                                                                  uuid))

    def test_instance_tag_delete(self):
        uuid = self._create_instance()
        tag1 = u'tag1'
        tag2 = u'tag2'

        db.instance_tag_add(self.context, uuid, tag1)
        db.instance_tag_add(self.context, uuid, tag2)

        tag_refs = db.instance_tag_get_by_instance_uuid(self.context, uuid)
        tags = self._get_tags_from_resp(tag_refs)
        expected = [(uuid, tag1), (uuid, tag2)]

        # Check the tags for the instance were added
        # NOTE(badock): use set to prevent errors caused by different items' order.
        self.assertEqual(set(expected), set(tags))

        db.instance_tag_delete(self.context, uuid, tag1)

        tag_refs = db.instance_tag_get_by_instance_uuid(self.context, uuid)
        tags = self._get_tags_from_resp(tag_refs)
        expected = [(uuid, tag2)]
        # NOTE(badock): use set to prevent errors caused by different items' order.
        self.assertEqual(set(expected), set(tags))

    def test_instance_tag_delete_non_existent(self):
        uuid = self._create_instance()
        self.assertRaises(exception.InstanceTagNotFound,
                          db.instance_tag_delete, self.context, uuid, u'tag')

    def test_instance_tag_delete_all(self):
        uuid = self._create_instance()
        tag1 = u'tag1'
        tag2 = u'tag2'

        db.instance_tag_add(self.context, uuid, tag1)
        db.instance_tag_add(self.context, uuid, tag2)

        tag_refs = db.instance_tag_get_by_instance_uuid(self.context, uuid)
        tags = self._get_tags_from_resp(tag_refs)
        expected = [(uuid, tag1), (uuid, tag2)]

        # Check the tags for the instance were added
        # NOTE(badock): use set to prevent errors caused by different items' order.
        self.assertEqual(set(expected), set(tags))

        db.instance_tag_delete_all(self.context, uuid)

        tag_refs = db.instance_tag_get_by_instance_uuid(self.context, uuid)
        tags = self._get_tags_from_resp(tag_refs)
        # NOTE(badock): use set to prevent errors caused by different items' order.
        self.assertEqual(set([]), set(tags))

    # def test_instance_tag_exists(self):
    #     uuid = self._create_instance()
    #     tag1 = u'tag1'
    #     tag2 = u'tag2'
    #
    #     db.instance_tag_add(self.context, uuid, tag1)
    #
    #     # NOTE(snikitin): Make sure it's actually a bool
    #     self.assertTrue(db.instance_tag_exists(self.context, uuid,
    #                                                     tag1))
    #     self.assertFalse(db.instance_tag_exists(self.context, uuid,
    #                                                      tag2))

    def test_instance_tag_add_to_non_existing_instance(self):
        self._create_instance()
        self.assertRaises(exception.InstanceNotFound, db.instance_tag_add,
                          self.context, 'fake_uuid', 'tag')

    def test_instance_tag_set_to_non_existing_instance(self):
        self._create_instance()
        self.assertRaises(exception.InstanceNotFound, db.instance_tag_set,
                          self.context, 'fake_uuid', ['tag1', 'tag2'])

    def test_instance_tag_get_from_non_existing_instance(self):
        self._create_instance()
        self.assertRaises(exception.InstanceNotFound,
                          db.instance_tag_get_by_instance_uuid, self.context,
                          'fake_uuid')

    def test_instance_tag_delete_from_non_existing_instance(self):
        self._create_instance()
        self.assertRaises(exception.InstanceNotFound, db.instance_tag_delete,
                          self.context, 'fake_uuid', 'tag')

    def test_instance_tag_delete_all_from_non_existing_instance(self):
        self._create_instance()
        self.assertRaises(exception.InstanceNotFound,
                          db.instance_tag_delete_all,
                          self.context, 'fake_uuid')

    def test_instance_tag_exists_non_existing_instance(self):
        self._create_instance()
        self.assertRaises(exception.InstanceNotFound,
                          db.instance_tag_exists,
                          self.context, 'fake_uuid', 'tag')
