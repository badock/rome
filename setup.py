# from distutils.core import setup
# from setuptools import setup, find_packages
#
# tests_require = [
#     'pytz',
#     'oslo.utils',
#     'oslo.db',
#     'pandas',
#     'pandasql',
#     'redis',
#     'redis-py-cluster',
#     'redlock-py',
#     'ujson',
#     'numexpr',
#     'sqlalchemy',
#     'gevent'
#     ]
#
# setup(
#     name='rome',
#     version='0.1',
#     tests_require=tests_require,
#     test_suite="execute_tests",
#     packages=['rome', 'rome.core', 'rome.core.orm', 'rome.core.dataformat', 'rome.driver', 'rome.driver.redis', 'rome.utils', 'rome.conf',
#               'rome.core.rows', 'rome.core.session', 'rome.tests', 'rome.tests.compatibility', 'rome.lang', 'rome.driver.etcd', 'rome.driver.etcd.driver'],
#     url='https://github.com/badock/rome',
#     license='',
#     author='jonathan',
#     author_email='',
#     description='Relational Object Mapping Extension for key/value stores'
# )
from setuptools import setup, find_packages

setup(name='rome', version='0.1.9', packages=find_packages(),)
