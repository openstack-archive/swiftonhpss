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

import hpss.clapi as hpss
from errno import ENOENT, EPERM, EEXIST, EINVAL
import posixpath

from swiftonhpss.swift.common.exceptions import SwiftOnFileSystemOSError

_auth_mech_to_api = {'unix': hpss.hpss_authn_mech_unix,
                     'krb5': hpss.hpss_authn_mech_krb5}

_cred_type_to_api = {'key': hpss.hpss_rpc_auth_type_key,
                     'keyfile': hpss.hpss_rpc_auth_type_keyfile,
                     'keytab': hpss.hpss_rpc_auth_type_keytab,
                     'password': hpss.hpss_rpc_auth_type_passwd}


# TODO: make a nicer wrapping around these things
def create_hpss_session(user_name,
                        auth_mech='unix',
                        auth_cred_type='keytab',
                        auth_cred='/var/hpss/etc/hpss.unix.keytab'):
    try:
        hpss_mech = _auth_mech_to_api[auth_mech]
        hpss_cred_type = _cred_type_to_api[auth_cred_type]
    except KeyError:
        raise ValueError('invalid mechanism or cred type specified')
    try:
        hpss.SetLoginCred(user_name,
                          hpss_mech,
                          hpss_cred_type,
                          hpss.hpss_rpc_cred_client,
                          auth_cred)
    except IOError as e:
        if e.errno == ENOENT:
            raise ValueError('HPSS reports invalid username')
        if e.errno == EPERM:
            raise OSError('Permission denied by HPSS')


def destroy_hpss_session():
    hpss.PurgeLoginCred()


def read_uda(path, key):
    normalized_key = '/hpss/' + key
    uda_contents = hpss.UserAttrGetAttrs(path,
                                         [normalized_key])[normalized_key]
    return hpss.ChompXMLHeader(uda_contents)


def write_uda(path, key, value):
    return hpss.UserAttrSetAttrs(path, {'/hpss/' + key: value})


def delete_uda(path, key):
    return hpss.UserAttrDeleteAttrs(path, '/hpss/' + key)


def list_uda(path):
    return hpss.UserAttrListAttrs(path)


def _split_path(path):
    if path == '/':
        return ['']
    else:
        return path.split('/')


def set_purge_lock(path, lock_status):
    try:
        fd = hpss.Open(path, hpss.O_RDWR | hpss.O_NONBLOCK)[0]
    except IOError as e:
        raise SwiftOnFileSystemOSError('couldnt open file for purgelock: %s' %
                                       e.errno)

    if lock_status:
        flag = hpss.PURGE_LOCK
    else:
        flag = hpss.PURGE_UNLOCK

    try:
        hpss.PurgeLock(int(fd), flag)
    except IOError as e:
        if e.errno != EINVAL:
            # This happens when either the file is empty, or there's no
            # tape layer in the class of service.
            # TODO: should we return an objection as an HTTP resp?
            raise SwiftOnFileSystemOSError('hpss.purge_lock(%s):'
                                           'fd %s, errno %s' %
                                           (path, fd, e.errno))

    try:
        hpss.Close(fd)
    except IOError as e:
        raise SwiftOnFileSystemOSError('couldnt close file for purgelock: %s' %
                                       e.errno)


def set_cos(path, cos_id):
    hpss.FileSetCOS(path, cos_id)


def preallocate(fd, size):
    hpss.Fpreallocate(fd, size)


def set_checksum(fd, md5_digest):
    flags = hpss.HPSS_FILE_HASH_GENERATED | hpss.HPSS_FILE_HASH_DIGEST_VALID
    hpss.FsetFileDigest(fd, md5_digest, "", flags, hpss.HASH_MD5)


def relative_symlink(parent_dir, source, target):
    pwd = bytes(hpss.Getcwd())
    hpss.Chdir(parent_dir)
    hpss.Symlink(source, target)
    hpss.Chdir(pwd)


def path_exists(path):
    try:
        return hpss.Access(path, hpss.F_OK)
    except IOError as e:
        if e.errno == ENOENT:
            return False
        else:
            raise


def makedirs(name, mode=0o777):
    components = posixpath.split(name)
    if components[0] != '/':
        makedirs(components[0], mode)
    try:
        hpss.Mkdir(name, mode)
    except IOError as e:
        if e.errno != EEXIST:
            raise


# TODO: give an iterator instead of a list of files/directories?
# that could quickly become way too many to list
def walk(path, topdown=True):
    dir_handle = hpss.Opendir(path)
    current_obj = hpss.Readdir(dir_handle)
    dirs = []
    files = []
    while current_obj:
        if current_obj.d_handle.Type == hpss.NS_OBJECT_TYPE_DIRECTORY:
            if current_obj.d_name != '.' and current_obj.d_name != '..':
                dirs.append(current_obj.d_name)
        elif current_obj.d_handle.Type == hpss.NS_OBJECT_TYPE_FILE:
            files.append(current_obj.d_name)
        current_obj = hpss.Readdir(dir_handle)
    hpss.Closedir(dir_handle)
    if topdown:
        yield (path, dirs, files)
        for dir_name in dirs:
            next_iter = walk("/".join(_split_path(path) + [dir_name]))
            for walk_entity in next_iter:
                yield walk_entity
    else:
        for dir_name in dirs:
            next_iter = walk("/".join(_split_path(path) + [dir_name]))
            for walk_entity in next_iter:
                yield walk_entity
        yield (path, dirs, files)


def _levels_to_string(storage_levels):
    def storage_info_string(level_struct):
        return '%u:%u:%u:%u' % (level_struct.BytesAtLevel,
                                level_struct.StripeLength,
                                level_struct.StripeWidth,
                                level_struct.OptimumAccessSize)

    def pv_list_string(pv_list):
        return ','.join([pv_element.Name for pv_element in pv_list])

    def vv_data_string(vv_struct):
        return '%s:%s:[%s]' % (vv_struct.BytesOnVV,
                               vv_struct.RelPosition,
                               pv_list_string(vv_struct.PVList))

    def vv_list_string(vv_list):
        return ':'.join(map(vv_data_string, vv_list))

    def more_exists_string(level_struct):
        if level_struct.Flags & hpss.BFS_BFATTRS_ADDITIONAL_VV_EXIST:
            return '...'
        else:
            return ''

    def level_data_string(level_struct, depth):
        if level_struct.Flags & hpss.BFS_BFATTRS_LEVEL_IS_DISK:
            storage_type = "disk"
        else:
            storage_type = "tape"
        if level_struct.BytesAtLevel == 0:
            return "%u:%s:nodata" % (depth, storage_type)
        else:
            return "%u:%s:%s:(%s)%s" % (depth,
                                        storage_type,
                                        storage_info_string(level_struct),
                                        vv_list_string(level_struct.VVAttrib),
                                        more_exists_string(level_struct))

    def level_list_string(levels):
        level_datas = []
        for index, level_struct in enumerate(levels):
            level_datas.append(level_data_string(level_struct, index))
        return ';'.join(level_datas)

    return level_list_string(storage_levels)


def read_hpss_system_metadata(path):
    flag = hpss.API_GET_STATS_FOR_ALL_LEVELS
    xfileattr_struct = hpss.FileGetXAttributes(path, flag)
    optimum_tuple = (xfileattr_struct.SCAttrib[0].OptimumAccessSize,
                     xfileattr_struct.SCAttrib[0].StripeWidth,
                     xfileattr_struct.SCAttrib[0].StripeLength)
    attrs = {'X-HPSS-Account': xfileattr_struct.Attrs.Account,
             'X-HPSS-Bitfile-ID': xfileattr_struct.Attrs.BitfileObj.BfId,
             'X-HPSS-Comment': xfileattr_struct.Attrs.Comment,
             'X-HPSS-Class-Of-Service-ID': xfileattr_struct.Attrs.COSId,
             'X-HPSS-Data-Levels':
                 _levels_to_string(xfileattr_struct.SCAttrib),
             'X-HPSS-Family-ID': xfileattr_struct.Attrs.FamilyId,
             'X-HPSS-Fileset-ID': xfileattr_struct.Attrs.FilesetId,
             'X-HPSS-Optimum-Size': "%u:%u:%u" % optimum_tuple,
             'X-HPSS-Purgelock-Status': xfileattr_struct.Attrs.OptionFlags & 1,
             'X-HPSS-Reads': xfileattr_struct.Attrs.ReadCount,
             'X-HPSS-Realm-ID': xfileattr_struct.Attrs.RealmId,
             'X-HPSS-Subsys-ID': xfileattr_struct.Attrs.SubSystemId,
             'X-HPSS-Writes': xfileattr_struct.Attrs.WriteCount, }
    return attrs
