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

import array
import fcntl

HPSSFS_GET_COS = 0x80046c01
HPSSFS_SET_COS_HINT = 0x40046c02

HPSSFS_SET_FSIZE_HINT = 0x40086c03
HPSSFS_SET_MAXSEGSZ_HINT = 0x40046c04

HPSSFS_PURGE_CACHE = 0x00006c05
HPSSFS_PURGE_LOCK = 0x40046c06

HPSSFS_UNDELETE = 0x40046c07
HPSSFS_UNDELETE_NONE = 0x00000000
HPSSFS_UNDELETE_RESTORE_TIME = 0x00000001
HPSSFS_UNDELETE_OVERWRITE = 0x00000002
HPSSFS_UNDELETE_OVERWRITE_AND_RESTORE = 0x00000003


def ioctl(fd, cmd, val=None):
    if val is not None:
        valbuf = array.array('i', [0])
    else:
        valbuf = array.array('i', [val])
    fcntl.ioctl(fd, cmd, valbuf)
    return valbuf[0]
