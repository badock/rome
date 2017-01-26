import logging
import unittest

from models import *
from nova import exception
import api as db
from utils import clean_rome_all_databases

import context as rome_context


def init_mock_objects(models_module):
    models_module.drop_tables()
    models_module.create_tables()
    models_module.init_objects()


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


class SecurityGroupRuleTestCase(unittest.TestCase, ModelsObjectComparatorMixin):
    def setUp(self):
        super(SecurityGroupRuleTestCase, self).setUp()
        self.ctxt = rome_context.get_admin_context()
        clean_rome_all_databases()

    def _get_base_values(self):
        return {
            'name': 'fake_sec_group',
            'description': 'fake_sec_group_descr',
            'user_id': 'fake',
            'project_id': 'fake',
            'instances': []
            }

    def _get_base_rule_values(self):
        return {
            'protocol': "tcp",
            'from_port': 80,
            'to_port': 8080,
            'cidr': None,
            'deleted': 0,
            'deleted_at': None,
            'grantee_group': None,
            'updated_at': None
            }

    def _test_security_group_rule_get_by_security_group(self, columns=None):
        instance = db.instance_create(self.ctxt,
                                      {'system_metadata': {'foo': 'bar'}})
        security_group = self._create_security_group({
                'instances': [instance]})
        security_group_rule = self._create_security_group_rule(
            {'parent_group': security_group, 'grantee_group': security_group})
        security_group_rule1 = self._create_security_group_rule(
            {'parent_group': security_group, 'grantee_group': security_group})
        found_rules = db.security_group_rule_get_by_security_group(
            self.ctxt, security_group['id'], columns_to_join=columns)
        self.assertEqual(len(found_rules), 2)
        rules_ids = [security_group_rule['id'], security_group_rule1['id']]
        for rule in found_rules:
            if columns is None:
                self.assertIn('grantee_group', dict(rule))
                self.assertIn('instances',
                              dict(rule.grantee_group))
                self.assertIn(
                    'system_metadata',
                    dict(rule.grantee_group.instances[0]))
                self.assertIn(rule['id'], rules_ids)
            else:
                self.assertNotIn('grantee_group', dict(rule))

    def _create_security_group(self, values):
        v = self._get_base_values()
        v.update(values)
        return db.security_group_create(self.ctxt, v)

    def _create_security_group_rule(self, values):
        v = self._get_base_rule_values()
        v.update(values)
        return db.security_group_rule_create(self.ctxt, v)

    def test_security_group_rule_create(self):
        security_group_rule = self._create_security_group_rule({})
        self.assertIsNotNone(security_group_rule['id'])
        for key, value in self._get_base_rule_values().items():
            self.assertEqual(value, security_group_rule[key])

    def test_security_group_rule_get_by_security_group(self):
        self._test_security_group_rule_get_by_security_group()

    # def test_security_group_rule_get_by_security_group_no_joins(self):
    #     self._test_security_group_rule_get_by_security_group(columns=[])

    def test_security_group_rule_get_by_instance(self):
        instance = db.instance_create(self.ctxt, {})
        security_group = self._create_security_group({
                'instances': [instance]})
        security_group_rule = self._create_security_group_rule(
            {'parent_group': security_group, 'grantee_group': security_group})
        security_group_rule1 = self._create_security_group_rule(
            {'parent_group': security_group, 'grantee_group': security_group})
        security_group_rule_ids = [security_group_rule['id'],
                                   security_group_rule1['id']]
        found_rules = db.security_group_rule_get_by_instance(self.ctxt,
                                                             instance['uuid'])
        self.assertEqual(len(found_rules), 2)
        for rule in found_rules:
            self.assertIn('grantee_group', rule)
            self.assertIn(rule['id'], security_group_rule_ids)

    def test_security_group_rule_destroy(self):
        self._create_security_group({'name': 'fake1'})
        self._create_security_group({'name': 'fake2'})
        security_group_rule1 = self._create_security_group_rule({})
        security_group_rule2 = self._create_security_group_rule({})
        db.security_group_rule_destroy(self.ctxt, security_group_rule1['id'])
        self.assertRaises(exception.SecurityGroupNotFound,
                          db.security_group_rule_get,
                          self.ctxt, security_group_rule1['id'])
        self._assertEqualObjects(db.security_group_rule_get(self.ctxt,
                                        security_group_rule2['id']),
                                 security_group_rule2, ['grantee_group'])

    def test_security_group_rule_destroy_not_found_exception(self):
        self.assertRaises(exception.SecurityGroupNotFound,
                          db.security_group_rule_destroy, self.ctxt, 100500)

    def test_security_group_rule_get(self):
        security_group_rule1 = (
                self._create_security_group_rule({}))
        self._create_security_group_rule({})
        real_security_group_rule = db.security_group_rule_get(self.ctxt,
                                              security_group_rule1['id'])
        self._assertEqualObjects(security_group_rule1,
                                 real_security_group_rule, ['grantee_group'])

    def test_security_group_rule_get_not_found_exception(self):
        self.assertRaises(exception.SecurityGroupNotFound,
                          db.security_group_rule_get, self.ctxt, 100500)

    def test_security_group_rule_count_by_group(self):
        sg1 = self._create_security_group({'name': 'fake1'})
        sg2 = self._create_security_group({'name': 'fake2'})
        rules_by_group = {sg1: [], sg2: []}
        for group in rules_by_group:
            rules = rules_by_group[group]
            for i in range(0, 10):
                rules.append(
                    self._create_security_group_rule({'parent_group_id':
                                                    group['id']}))
        db.security_group_rule_destroy(self.ctxt,
                                       rules_by_group[sg1][0]['id'])
        counted_groups = [db.security_group_rule_count_by_group(self.ctxt,
                                                                group['id'])
                          for group in [sg1, sg2]]
        expected = [9, 10]
        self.assertEqual(counted_groups, expected)

if __name__ == '__main__':
    unittest.main()
