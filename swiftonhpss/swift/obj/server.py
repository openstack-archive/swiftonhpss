# Copyright (c) 2012-2014 Red Hat, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

""" Object Server for Gluster for Swift """

import math
import logging
import time
import xattr
import os
import hpssfs
from hashlib import md5
from swift.common.swob import HTTPConflict, HTTPBadRequest, HeaderKeyDict, \
    HTTPInsufficientStorage, HTTPPreconditionFailed, HTTPRequestTimeout, \
    HTTPClientDisconnect, HTTPUnprocessableEntity, HTTPNotImplemented, \
    HTTPServiceUnavailable, HTTPCreated, HTTPNotFound, HTTPAccepted, \
    HTTPNoContent, Request, Response
from swift.common.utils import public, timing_stats, replication, \
    config_true_value, Timestamp, csv_append
from swift.common.request_helpers import get_name_and_placement, \
    split_and_validate_path, is_sys_or_user_meta
from swiftonhpss.swift.common.exceptions import AlreadyExistsAsFile, \
    AlreadyExistsAsDir, SwiftOnFileSystemIOError, SwiftOnFileSystemOSError, \
    SwiftOnFileFsException
from swift.common.exceptions import DiskFileDeviceUnavailable, \
    DiskFileNotExist, DiskFileQuarantined, ChunkReadTimeout, DiskFileNoSpace, \
    DiskFileXattrNotSupported, DiskFileExpired, DiskFileDeleted
from swift.common.constraints import valid_timestamp, check_account_format, \
    check_destination_header

from swift.obj import server

from swiftonhpss.swift.obj.diskfile import DiskFileManager
from swiftonhpss.swift.common.constraints import check_object_creation
from swiftonhpss.swift.common import utils


class SwiftOnFileDiskFileRouter(object):
    """
    Replacement for Swift's DiskFileRouter object.
    Always returns SwiftOnFile's DiskFileManager implementation.
    """

    def __init__(self, *args, **kwargs):
        self.manager_cls = DiskFileManager(*args, **kwargs)

    def __getitem__(self, policy):
        return self.manager_cls


class ObjectController(server.ObjectController):
    """
    Subclass of the object server's ObjectController that supports HPSS-specific
    metadata headers and operations (such as COS assignment and purge locking).
    """

    def setup(self, conf):
        """
        Implementation specific setup. This method is called at the very end
        by the constructor to allow a specific implementation to modify
        existing attributes or add its own attributes.

        :param conf: WSGI configuration parameter
        """
        # Replaces Swift's DiskFileRouter object reference with ours.
        self._diskfile_router = SwiftOnFileDiskFileRouter(conf, self.logger)
        # This conf option will be deprecated and eventualy removed in
        # future releases
        utils.read_pickled_metadata = \
            config_true_value(conf.get('read_pickled_metadata', 'no'))

    @public
    @timing_stats()
    def PUT(self, request):
        """Handle HTTP PUT requests for the Swift on File object server"""
        try:
            device, partition, account, container, obj, policy = \
                get_name_and_placement(request, 5, 5, True)

            req_timestamp = valid_timestamp(request)

            # check swiftonhpss constraints first
            error_response = check_object_creation(request, obj)
            if error_response:
                return error_response

            # (HPSS) Shameless copy-paste from ObjectController.PUT and
            # modification, because we have to do certain things like pass in
            # purgelock and class-of-service information that Swift won't know
            # to do and need to do it in a very specific order.
            new_delete_at = int(request.headers.get('X-Delete-At') or 0)
            if new_delete_at and new_delete_at < time.time():
                return HTTPBadRequest(body='X-Delete-At in past',
                                      request=request,
                                      context_type='text/plain')

            try:
                fsize = request.message_length()
            except ValueError as e:
                return HTTPBadRequest(body=str(e),
                                      request=request,
                                      content_type='text/plain')

            # Try to get DiskFile
            try:
                disk_file = self.get_diskfile(device, partition, account,
                                              container, obj, policy=policy)
            except DiskFileDeviceUnavailable:
                return HTTPInsufficientStorage(drive=device, request=request)

            try:
                orig_metadata = disk_file.read_metadata()
            except (DiskFileNotExist, DiskFileQuarantined):
                orig_metadata = {}

            # Check for If-None-Match in request
            if request.if_none_match and orig_metadata:
                if '*' in request.if_none_match:
                    # File exists already, return 412
                    return HTTPPreconditionFailed(request=request)
                if orig_metadata.get('ETag') in request.if_none_match:
                    # The current ETag matches, return 412
                    return HTTPPreconditionFailed(request=request)

            orig_timestamp = Timestamp(orig_metadata.get('X-Timestamp', 0))

            if orig_timestamp >= req_timestamp:
                return HTTPConflict(
                    request=request,
                    headers={'X-Backend-Timestamp': orig_timestamp.internal})
            orig_delete_at = int(orig_metadata.get('X-Delete-At') or 0)
            upload_expiration = time.time() + self.max_upload_time

            etag = md5()
            elapsed_time = 0

            # (HPSS) Check for HPSS-specific metadata headers
            cos = request.headers.get('X-Object-Meta-COS')
            purgelock = request.headers.get('X-Object-Meta-PurgeLock')

            try:
                # Feed DiskFile our HPSS-specific stuff
                with disk_file.create(size=fsize, cos=cos) as writer:
                    upload_size = 0
                    # FIXME: Need to figure out how to store MIME type
                    # information, to retrieve with a GET later! Or if
                    # this has already been done for us.

                    def timeout_reader():
                        with ChunkReadTimeout(self.client_timeout):
                            return request.environ['wsgi.input'].read(
                                self.network_chunk_size)

                    try:
                        for chunk in iter(lambda: timeout_reader(), ''):
                            start_time = time.time()
                            if start_time > upload_expiration:
                                self.logger.increment('PUT.timeouts')
                                return HTTPRequestTimeout(request=request)
                            etag.update(chunk)
                            upload_size = writer.write(chunk)
                            elapsed_time += time.time() - start_time
                    except ChunkReadTimeout:
                        return HTTPRequestTimeout(request=request)
                    if upload_size:
                        self.logger.transfer_rate('PUT.%s.timing' % device,
                                                  elapsed_time, upload_size)
                    if fsize and fsize != upload_size:
                        return HTTPClientDisconnect(request=request)
                    etag = etag.hexdigest()
                    if 'etag' in request.headers \
                            and request.headers['etag'].lower() != etag:
                        return HTTPUnprocessableEntity(request=request)

                    # Update object metadata
                    metadata = {'X-Timestamp': request.timestamp.internal,
                                'Content-Type': request.headers['content-type'],
                                'ETag': etag,
                                'Content-Length': str(upload_size),
                                }
                    metadata.update(
                        val for val in request.headers.iteritems()
                        if is_sys_or_user_meta('object', val[0]))
                    backend_headers = \
                        request.headers.get('X-Backend-Replication-Headers')
                    for header_key in (backend_headers or self.allowed_headers):
                        if header_key in request.headers:
                            header_caps = header_key.title()
                            metadata[header_caps] = request.headers[header_key]

                    # (HPSS) Purge lock the file
                    writer.put(metadata, purgelock=purgelock)

            except DiskFileNoSpace:
                return HTTPInsufficientStorage(drive=device, request=request)
            except SwiftOnFileSystemIOError:
                return HTTPServiceUnavailable(request=request)

            # (HPSS) Set checksum on file
            try:
                xattr.setxattr(disk_file._data_file, 'system.hpss.hash',
                               "md5:%s" % etag)
            except IOError:
                logging.exception("Error setting HPSS E2EDI checksum in "
                                  "system.hpss.hash, storing ETag in "
                                  "user.hash.checksum\n")
                try:
                    xattr.setxattr(disk_file._data_file,
                                   'user.hash.checksum', etag)
                    xattr.setxattr(disk_file._data_file,
                                   'user.hash.algorithm', 'md5')
                    xattr.setxattr(disk_file._data_file,
                                   'user.hash.state', 'Valid')
                    xattr.setxattr(disk_file._data_file,
                                   'user.hash.filesize', str(upload_size))
                except IOError as err:
                    raise SwiftOnFileSystemIOError(
                        err.errno, '%s, xattr.setxattr(...)' % err.strerror)

            # Update container metadata
            if orig_delete_at != new_delete_at:
                if new_delete_at:
                    self.delete_at_update('PUT', new_delete_at, account,
                                          container, obj, request, device,
                                          policy)
                if orig_delete_at:
                    self.delete_at_update('DELETE', orig_delete_at, account,
                                          container, obj, request, device,
                                          policy)
            self.container_update('PUT', account, container, obj, request,
                                  HeaderKeyDict(
                                      {'x-size':
                                           metadata['Content-Length'],
                                       'x-content-type':
                                           metadata['Content-Type'],
                                       'x-timestamp':
                                           metadata['X-Timestamp'],
                                       'x-etag':
                                           metadata['ETag']}),
                                  device, policy)
            # Create convenience symlink
            try:
                self.object_symlink(request, disk_file._data_file, device,
                                    account)
            except SwiftOnFileSystemOSError:
                return HTTPServiceUnavailable(request=request)
            return HTTPCreated(request=request, etag=etag)

        except (AlreadyExistsAsFile, AlreadyExistsAsDir):
            device = \
                split_and_validate_path(request, 1, 5, True)
            return HTTPConflict(drive=device, request=request)

    def object_symlink(self, request, diskfile, device, account):
        mount = diskfile.split(device)[0]
        dev = "%s%s" % (mount, device)
        project = None
        if 'X-Project-Name' in request.headers:
            project = request.headers.get('X-Project-Name')
        elif 'X-Tenant-Name' in request.headers:
            project = request.headers.get('X-Tenant-Name')
        if project:
            if project is not account:
                accdir = "%s/%s" % (dev, account)
                projdir = "%s%s" % (mount, project)
                if not os.path.exists(projdir):
                    try:
                        os.symlink(accdir, projdir)
                    except OSError as err:
                        raise SwiftOnFileSystemOSError(
                            err.errno,
                            ('%s, os.symlink("%s", ...)' %
                             err.strerror, account))

    @public
    @timing_stats()
    def HEAD(self, request):
        """Handle HTTP HEAD requests for the Swift on File object server"""
        device, partition, account, container, obj, policy = \
            get_name_and_placement(request, 5, 5, True)

        # Get DiskFile
        try:
            disk_file = self.get_diskfile(device, partition, account, container,
                                          obj, policy=policy)
        except DiskFileDeviceUnavailable:
            return HTTPInsufficientStorage(drive=device, request=request)

        # Read DiskFile metadata
        try:
            metadata = disk_file.read_metadata()
        except (DiskFileNotExist, DiskFileQuarantined) as e:
            headers = {}
            if hasattr(e, 'timestamp'):
                headers['X-Backend-Timestamp'] = e.timestamp.internal
            return HTTPNotFound(request=request, headers=headers,
                                conditional_respose=True)

        # Create and populate our response
        response = Response(request=request, conditional_response=True)
        response.headers['Content-Type'] = \
            metadata.get('Content-Type', 'application/octet-stream')
        for key, value in metadata.iteritems():
            if is_sys_or_user_meta('object', key) or key.lower() in \
                    self.allowed_headers:
                response.headers[key] = value
        response.etag = metadata['ETag']
        ts = Timestamp(metadata['X-Timestamp'])

        # Needed for container sync feature
        response.headers['X-Timestamp'] = ts.normal
        response.headers['X-Backend-Timestamp'] = ts.internal
        response.content_length = int(metadata['Content-Length'])
        try:
            response.content_encoding = metadata['Content-Encoding']
        except KeyError:
            pass

        try:
            self.get_hpss_xattr(request, response, disk_file)
        except SwiftOnFileSystemIOError:
            return HTTPServiceUnavailable(request=request)

        # Bill Owen's hack to force container sync on HEAD, so we can manually
        # tell the Swift container server when objects exist on disk it didn't
        # know about.
        # TODO: do a similar trick for HEADing objects that didn't exist
        # TODO: see if this block that's duplicated can be a function instead
        if 'X-Object-Sysmeta-Update-Container' in response.headers:
            self.container_update(
                'PUT', account, container, obj, request,
                HeaderKeyDict(
                    {'x-size': metadata['Content-Length'],
                     'x-content-type': metadata['Content-Type'],
                     'x-timestamp': metadata['X-Timestamp'],
                     'x-etag': metadata['ETag']
                     }
                ),
                device, policy)
            response.headers.pop('X-Object-Sysmeta-Update-Container')

        return response

    @public
    @timing_stats()
    def GET(self, request):
        """Handle HTTP GET requests for the Swift on File object server"""
        device, partition, account, container, obj, policy = \
            get_name_and_placement(request, 5, 5, True)
        keep_cache = self.keep_cache_private or (
            'X-Auth-Token' not in request.headers and
            'X-Storage-Token' not in request.headers
        )

        # Get Diskfile
        try:
            disk_file = self.get_diskfile(device, partition, account, container,
                                          obj, policy)
        except DiskFileDeviceUnavailable:
            return HTTPInsufficientStorage(drive=device, request=request)

        # Get metadata and append it to response
        try:
            with disk_file.open():
                metadata = disk_file.get_metadata()
                obj_size = int(metadata['Content-Length'])
                file_x_ts = Timestamp(metadata['X-Timestamp'])
                try:
                    # (HPSS) Our file could end up being on an offline
                    # tape, so we need to check for it and return an
                    # HTTP 'accepted, but still processing' response.
                    if self.is_offline(disk_file._data_file, request):
                        return HTTPAccepted(request=request)
                except (SwiftOnFileSystemIOError, SwiftOnFileFsException):
                    return HTTPServiceUnavailable(request=request)

                response = Response(
                    app_iter=disk_file.reader(keep_cache=keep_cache),
                    request=request, conditional_response=True
                )
                response.headers['Content-Type'] = metadata.get(
                    'Content-Type', 'application/octet-stream'
                )
                for key, value in metadata.iteritems():
                    if is_sys_or_user_meta('object', key) or \
                                    key.lower() in self.allowed_headers:
                        response.headers[key] = value
                response.etag = metadata['ETag']
                response.last_modified = math.ceil(float(file_x_ts))
                response.content_length = obj_size
                try:
                    response.content_encoding = metadata['Content-Encoding']
                except KeyError:
                    pass
                response.headers['X-Timestamp'] = file_x_ts.normal
                response.headers['X-Backend-Timestamp'] = file_x_ts.internal
                # (HPSS) Inject HPSS xattr metadata into headers
                try:
                    self.get_hpss_xattr(request, response, disk_file)
                except SwiftOnFileSystemIOError:
                    return HTTPServiceUnavailable(request=request)
                return request.get_response(response)
        except (DiskFileNotExist, DiskFileQuarantined) as e:
            headers = {}
            if hasattr(e, 'timestamp'):
                headers['X-Backend-Timestamp'] = e.timestamp.internal
                return HTTPNotFound(request=request, headers=headers,
                                    conditional_response=True)

    # TODO: refactor this to live in DiskFile!
    # Along with all the other HPSS stuff
    def get_hpss_xattr(self, request, response, diskfile):
        attrlist = {'X-HPSS-Account': 'account',
                    'X-HPSS-BitfileID': 'bitfile',
                    'X-HPSS-Comment': 'comment',
                    'X-HPSS-ClassOfServiceID': 'cos',
                    'X-HPSS-FamilyID': 'family',
                    'X-HPSS-FilesetID': 'fileset',
                    'X-HPSS-Bytes': 'level',
                    'X-HPSS-Reads': 'reads',
                    'X-HPSS-RealmID': 'realm',
                    'X-HPSS-SubsysID': 'subsys',
                    'X-HPSS-Writes': 'writes',
                    'X-HPSS-OptimumSize': 'optimum',
                    'X-HPSS-Hash': 'hash',
                    'X-HPSS-PurgelockStatus': 'purgelock'}
        for key in request.headers:
            val = attrlist.get(key, None)
            if val:
                attr = 'system.hpss.%s' % val
                try:
                    response.headers[key] = \
                        xattr.getxattr(diskfile._data_file, attr)
                except IOError as err:
                    raise SwiftOnFileSystemIOError(
                        err.errno,
                        '%s, xattr.getxattr("%s", ...)' % (err.strerror, attr)
                    )

    # TODO: move this to DiskFile
    # TODO: make it more obvious how we're parsing the level xattr
    def is_offline(self, path, request):
        try:
            byteslevel = xattr.getxattr(path, "system.hpss.level")
        except IOError as err:
            raise SwiftOnFileSystemIOError(
                err.errno,
                '%s, xattr.getxattr("system.hpss.level", ...)' % err.strerror
            )

        try:
            byteslevelstring = byteslevel.split(";")
            bytesfirstlevel = byteslevelstring[0].split(':')
            bytesfile = bytesfirstlevel[2].rstrip(' ')
        except ValueError:
            raise SwiftOnFileFsException("Couldn't get system.hpss.level!")
        setbytes = set(str(bytesfile))
        setsize = set(str(os.stat(path).st_size))
        return setbytes != setsize

    @public
    @timing_stats()
    def POST(self, request):
        """Handle HTTP POST requests for the Swift on File object server"""
        device, partition, account, container, obj, policy = \
            get_name_and_placement(request, 5, 5, True)
        req_timestamp = valid_timestamp(request)
        new_delete_at = int(request.headers.get('X-Delete-At') or 0)
        if new_delete_at and new_delete_at < time.time():
            return HTTPBadRequest(body='X-Delete-At in past', request=request,
                                  content_type='text/plain')

        # Get DiskFile
        try:
            disk_file = self.get_diskfile(device, partition, account,
                                          container, obj, policy)
        except DiskFileDeviceUnavailable:
            return HTTPInsufficientStorage(drive=device, request=request)

        # Set Purgelock status if we got it
        purgelock = request.headers.get('X-Object-Meta-PurgeLock')
        if purgelock:
            try:
                hpssfs.ioctl(disk_file._fd, hpssfs.HPSSFS_PURGE_LOCK,
                             int(purgelock))
            except IOError as err:
                raise SwiftOnFileSystemIOError(
                    err.errno,
                    '%s, xattr.getxattr("%s", ...)' %
                    (err.strerror, disk_file._fd))

        # Update metadata from request
        try:
            orig_metadata = disk_file.read_metadata()
        except (DiskFileNotExist, DiskFileQuarantined):
            return HTTPNotFound(request=request)
        orig_timestamp = Timestamp(orig_metadata.get('X-Timestamp', 0))
        if orig_timestamp >= req_timestamp:
            return HTTPConflict(request=request,
                                headers={
                                    'X-Backend-Timestamp': orig_timestamp.internal
                                })
        metadata = {'X-Timestamp': req_timestamp.internal}
        metadata.update(val for val in request.headers.iteritems()
                        if is_user_meta('object', val[0]))
        for header_key in self.allowed_headers:
            if header_key in request.headers:
                header_caps = header_key.title()
                metadata[header_caps] = request.headers[header_key]
        orig_delete_at = int(orig_metadata.get('X-Delete-At') or 0)
        if orig_delete_at != new_delete_at:
            if new_delete_at:
                self.delete_at_update('PUT', new_delete_at, account,
                                      container, obj, request, device, policy)
            if orig_delete_at:
                self.delete_at_update('DELETE', orig_delete_at, account,
                                      container, obj, request, device, policy)
        disk_file.write_metadata(metadata)
        return HTTPAccepted(request=request)

    @public
    @timing_stats()
    def DELETE(self, request):
        """Handle HTTP DELETE requests for the Swift on File object server"""
        device, partition, account, container, obj, policy = \
            get_name_and_placement(request, 5, 5, True)
        req_timestamp = valid_timestamp(request)
        try:
            disk_file = self.get_diskfile(device, partition, account,
                                          container, obj, policy)
        except DiskFileDeviceUnavailable:
            return HTTPInsufficientStorage(drive=device, request=request)

        try:
            orig_metadata = disk_file.read_metadata()
        except DiskFileXattrNotSupported:
            return HTTPInsufficientStorage(drive=device, request=request)
        except DiskFileExpired as e:
            orig_timestamp = e.timestamp
            orig_metadata = e.metadata
            response_class = HTTPNotFound
        except DiskFileDeleted:
            orig_timestamp = e.timestamp
            orig_metadata = {}
            response_class = HTTPNotFound
        except (DiskFileNotExist, DiskFileQuarantined):
            orig_timestamp = 0
            orig_metadata = {}
            response_class = HTTPNotFound
        else:
            orig_timestamp = Timestamp(orig_metadata.get('X-Timestamp', 0))
            if orig_timestamp < req_timestamp:
                response_class = HTTPNoContent
            else:
                response_class = HTTPConflict
        response_timestamp = max(orig_timestamp, req_timestamp)
        orig_delete_at = int(orig_metadata.get('X-Delete-At') or 0)
        try:
            req_if_delete_at = int(request.headers['X-If-Delete-At'])
        except KeyError:
            pass
        except ValueError:
            return HTTPBadRequest(request=request,
                                  body='Bad X-If-Delete-At header value')
        else:
            if not orig_timestamp:
                return HTTPNotFound()
            if orig_delete_at != req_if_delete_at:
                return HTTPPreconditionFailed(
                    request=request,
                    body='X-If-Delete-At and X-Delete-At do not match')
            else:
                response_class = HTTPNoContent
        if orig_delete_at:
            self.delete_at_update('DELETE', orig_delete_at, account,
                                  container, obj, request, device, policy)
        if orig_timestamp < req_timestamp:
            disk_file.delete(req_timestamp)
            self.container_update('DELETE', account, container, obj, request,
                                  HeaderKeyDict(
                                      {'x-timestamp': req_timestamp.internal}
                                  ), device, policy)
        return response_class(
            request=request,
            headers={'X-Backend-Timestamp': response_timestamp.internal}
        )

    @public
    @replication
    @timing_stats(sample_rate=0.1)
    def REPLICATE(self, request):
        """
        In Swift, this method handles REPLICATE requests for the Swift
        Object Server.  This is used by the object replicator to get hashes
        for directories.

        Swiftonfile does not support this as it expects the underlying
        filesystem to take care of replication. Also, swiftonhpss has no
        notion of hashes for directories.
        """
        return HTTPNotImplemented(request=request)

    @public
    @replication
    @timing_stats(sample_rate=0.1)
    def REPLICATION(self, request):
        return HTTPNotImplemented(request=request)


def app_factory(global_conf, **local_conf):
    """paste.deploy app factory for creating WSGI object server apps"""
    conf = global_conf.copy()
    conf.update(local_conf)
    return ObjectController(conf)
