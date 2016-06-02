# Copyright (c) 2016 IBM Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from test.functional.tests import Base, Utils
from test.functional.swift_test_client import Account, Connection, \
    ResponseError
import test.functional as tf
import time
import logging
import os
import unittest
import xattr


class TestSwiftOnHPSS(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.connection = Connection(tf.config)
        cls.connection.authenticate()
        cls.account = Account(cls.connection,
                              tf.config.get('account',
                                            tf.config['username']))
        cls.container = cls.account.container('swiftonhpss_test')
        cls.container.create(hdrs={'X-Storage-Policy': 'hpss'})
        cls.hpss_dir = '/srv/swift/hpss'

    @classmethod
    def tearDownClass(cls):
        cls.container.delete()

    def setUp(self):
        self.test_file = self.container.file('testfile')

    def tearDown(self):
        self.test_file.delete()

    def test_purge_lock(self):
        self.test_file.write(data='test',
                             hdrs={'X-Hpss-Purgelock-Status': 'true',
                                   'X-Hpss-Class-Of-Service-Id': '3'})

        test_file_name = os.path.join(self.hpss_dir,
                                      self.account.name,
                                      self.container.name,
                                      'testfile')

        xattrs = dict(xattr.xattr(test_file_name))
        self.assertEqual(xattrs['system.hpss.purgelock'], '1')

        self.test_file.post(hdrs={'X-Hpss-Purgelock-Status': 'false'})
        xattrs = dict(xattr.xattr(test_file_name))
        self.assertEqual(xattrs['system.hpss.purgelock'], '0')

    def test_change_cos(self):
        self.test_file.write(data='asdfasdf',
                             hdrs={'X-Hpss-Class-Of-Service-Id': '3'})

        test_file_name = os.path.join(self.hpss_dir,
                                      self.account.name,
                                      self.container.name,
                                      'testfile')

        time.sleep(30)  # It takes a long time for HPSS to get around to it.
        xattrs = dict(xattr.xattr(test_file_name))
        self.assertEqual(xattrs['system.hpss.cos'], '3')

        self.test_file.post(hdrs={'X-HPSS-Class-Of-Service-ID': '1'})
        time.sleep(30)
        xattrs = dict(xattr.xattr(test_file_name))
        self.assertEqual(xattrs['system.hpss.cos'], '1')

    def test_hpss_metadata(self):
        # header is X-HPSS-Get-Metadata
        self.test_file.write(data='test')
        self.connection.make_request('HEAD', self.test_file.path,
                                     hdrs={'X-HPSS-Get-Metadata': 'true'})
        md = {t[0]: t[1] for t in self.connection.response.getheaders()}
        print md
        self.assertTrue('x-hpss-account' in md)
        self.assertTrue('x-hpss-bitfile-id' in md)
        self.assertTrue('x-hpss-comment' in md)
        self.assertTrue('x-hpss-class-of-service-id' in md)
        self.assertTrue('x-hpss-data-levels' in md)
        self.assertTrue('x-hpss-family-id' in md)
        self.assertTrue('x-hpss-fileset-id' in md)
        self.assertTrue('x-hpss-optimum-size' in md)
        self.assertTrue('x-hpss-purgelock-status' in md)
        self.assertTrue('x-hpss-reads' in md)
        self.assertTrue('x-hpss-realm-id' in md)
        self.assertTrue('x-hpss-subsys-id' in md)
        self.assertTrue('x-hpss-writes' in md)