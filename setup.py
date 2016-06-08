# Copyright (c) 2016 IBM Corporation
# Copyright (c) 2013 Red Hat, Inc.
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

from setuptools import setup, find_packages
from swiftonhpss.swift import _pkginfo
import os
import sys
import shutil

setup(
    name=_pkginfo.name,
    version=_pkginfo.full_version,
    description="Swift-on-HPSS is a fork of the Swift-on-File Swift"
                " Object Server implementation that enables users to"
                " access the same data, both as an object and as a file."
                " Data can be stored and retrieved through Swift's REST"
                " interface or as files from your site's HPSS archive system.",
    license='Apache License (2.0)',
    author='HPSS Collaboration',
    url='https://github.com/openstack/swiftonhpss',
    packages=find_packages(exclude=['test', 'bin', 'system_test']),
    test_suite='nose.collector',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: OpenStack',
        'Intended Audience :: Information Technology',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7'
    ],
    install_requires=[],
    scripts=[
        'bin/swiftonhpss-print-metadata',
        'bin/swiftonhpss-migrate-metadata',
        'bin/swiftonhpss-nstool'
    ],
    entry_points={
        'paste.app_factory': [
            'object=swiftonhpss.swift.obj.server:app_factory',
        ],
        'paste.filter_factory': [
            'sof_constraints=swiftonhpss.swift.common.middleware.'
            'check_constraints:filter_factory',
        ],
    },
)

if 'install' in sys.argv:
    # Install man pages the crappy hacky way, because setuptools doesn't
    # have any facility to do it.
    man_path = '/usr/local/share/man/1'
    man_pages = filter(lambda x: os.path.isfile('./doc/troff/%s' % x),
                       os.listdir('./doc/troff'))
    for page in man_pages:
        shutil.copyfile('./doc/troff/%s' % page, man_path)
