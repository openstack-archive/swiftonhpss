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
import xattr
import os
try:
    import hpssfs
except ImportError:
    import swiftonhpss.swift.common.hpssfs_ioctl as hpssfs
import time

from hashlib import md5
from swift.common.swob import HTTPConflict, HTTPBadRequest, HeaderKeyDict, \
    HTTPInsufficientStorage, HTTPPreconditionFailed, HTTPRequestTimeout, \
    HTTPClientDisconnect, HTTPUnprocessableEntity, HTTPNotImplemented, \
    HTTPServiceUnavailable, HTTPCreated, HTTPNotFound, HTTPAccepted, \
    HTTPNoContent, Response
from swift.common.utils import public, timing_stats, replication, \
    config_true_value, Timestamp, csv_append
from swift.common.request_helpers import get_name_and_placement, \
    split_and_validate_path, is_sys_or_user_meta, is_user_meta
from swiftonhpss.swift.common.exceptions import AlreadyExistsAsFile, \
    AlreadyExistsAsDir, SwiftOnFileSystemIOError, SwiftOnFileSystemOSError, \
    SwiftOnFileFsException
from swift.common.exceptions import DiskFileDeviceUnavailable, \
    DiskFileNotExist, DiskFileQuarantined, ChunkReadTimeout, DiskFileNoSpace, \
    DiskFileXattrNotSupported, DiskFileExpired, DiskFileDeleted
from swift.common.constraints import valid_timestamp
from swift.obj import server
from swift.common.ring import Ring

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
    Subclass of the object server's ObjectController that supports
    HPSS-specific metadata headers and operations (such as COS assignment
    and purge locking).
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
        self.swift_dir = conf.get('swift_dir', '/etc/swift')
        self.container_ring = None
        # This conf option will be deprecated and eventualy removed in
        # future releases
        utils.read_pickled_metadata = \
            config_true_value(conf.get('read_pickled_metadata', 'no'))

    def get_container_ring(self):
        """Get the container ring.  Load it, if it hasn't been yet."""
        if not self.container_ring:
            self.container_ring = Ring(self.swift_dir, ring_name='container')
        return self.container_ring

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
            cos = request.headers.get('X-Hpss-Class-Of-Service-Id', None)
            purgelock = config_true_value(
                request.headers.get('X-Hpss-Purgelock-Status', 'false'))

            try:
                # Feed DiskFile our HPSS-specific stuff
                with disk_file.create(size=fsize, cos=cos) as writer:
                    upload_size = 0

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
                    content_type = request.headers['content-type']
                    metadata = {'X-Timestamp': request.timestamp.internal,
                                'Content-Type': content_type,
                                'ETag': etag,
                                'Content-Length': str(upload_size),
                                }
                    meta_headers = {header: request.headers[header] for header
                                    in request.headers
                                    if is_sys_or_user_meta('object', header)}
                    metadata.update(meta_headers)
                    backend_headers = \
                        request.headers.get('X-Backend-Replication-Headers')
                    for header_key in (backend_headers or
                                       self.allowed_headers):
                        if header_key in request.headers:
                            header_caps = header_key.title()
                            metadata[header_caps] = request.headers[header_key]

                    # (HPSS) Write the file, with added options
                    writer.put(metadata, purgelock=purgelock)

            except DiskFileNoSpace:
                return HTTPInsufficientStorage(drive=device, request=request)
            except SwiftOnFileSystemIOError as e:
                return HTTPServiceUnavailable(request=request)

            # FIXME: this stuff really should be handled in DiskFile somehow?
            # we set the hpss checksum in here, so both systems have valid
            # and current checksum metadata

            # (HPSS) Set checksum on file ourselves, if hpssfs won't do it
            # for us.
            data_file = disk_file._data_file
            try:
                xattr.setxattr(data_file, 'system.hpss.hash',
                               "md5:%s" % etag)
            except IOError:
                logging.debug("Could not write ETag to system.hpss.hash,"
                              " trying user.hash.checksum")
                try:
                    xattr.setxattr(data_file,
                                   'user.hash.checksum', etag)
                    xattr.setxattr(data_file,
                                   'user.hash.algorithm', 'md5')
                    xattr.setxattr(data_file,
                                   'user.hash.state', 'Valid')
                    xattr.setxattr(data_file,
                                   'user.hash.filesize', str(upload_size))
                    xattr.setxattr(data_file,
                                   'user.hash.app', 'swiftonhpss')
                except IOError as err:
                    raise SwiftOnFileSystemIOError(
                        err.errno,
                        'Could not write MD5 checksum to HPSS filesystem: '
                        '%s' % err.strerror)

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
            container_headers = {'x-size': metadata['Content-Length'],
                                 'x-content-type': metadata['Content-Type'],
                                 'x-timestamp': metadata['X-Timestamp'],
                                 'x-etag': metadata['ETag']}
            self.container_update('PUT', account, container, obj, request,
                                  HeaderKeyDict(container_headers),
                                  device, policy)
            # Create convenience symlink
            try:
                self._object_symlink(request, disk_file._data_file, device,
                                     account)
            except SwiftOnFileSystemOSError:
                logging.debug('could not make account symlink')
                return HTTPServiceUnavailable(request=request)
            return HTTPCreated(request=request, etag=etag)

        except (AlreadyExistsAsFile, AlreadyExistsAsDir):
            device = \
                split_and_validate_path(request, 1, 5, True)
            return HTTPConflict(drive=device, request=request)

    def _sof_container_update(self, request, resp):
        """
        SOF specific metadata is set in DiskFile.open()._filter_metadata()
        This method internally invokes Swift's container_update() method.
        """
        device, partition, account, container, obj, policy_idx = \
            get_name_and_placement(request, 5, 5, True)

        # The container_update() method requires certain container
        # specific headers. The proxy object controller appends these
        # headers for PUT backend request but not for HEAD/GET requests.
        # Thus, we populate the required information in request
        # and then invoke container_update()
        container_partition, container_nodes = \
            self.get_container_ring().get_nodes(account, container)
        request.headers['X-Container-Partition'] = container_partition
        for node in container_nodes:
            request.headers['X-Container-Host'] = csv_append(
                request.headers.get('X-Container-Host'),
                '%(ip)s:%(port)s' % node)
            request.headers['X-Container-Device'] = csv_append(
                request.headers.get('X-Container-Device'), node['device'])

        self.container_update(
            'PUT', account, container, obj, request,
            HeaderKeyDict({
                'x-size': resp.headers['Content-Length'],
                'x-content-type': resp.headers['Content-Type'],
                'x-timestamp': resp.headers['X-Timestamp'],
                'x-etag': resp.headers['ETag']}),
            device, policy_idx)

    def _object_symlink(self, request, diskfile, device, account):
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
            disk_file = self.get_diskfile(device, partition, account,
                                          container, obj, policy=policy)

        except DiskFileDeviceUnavailable:
            return HTTPInsufficientStorage(drive=device, request=request)

        hpss_headers = None

        # Read DiskFile metadata
        try:
            with disk_file.open():
                metadata = disk_file.get_metadata()
                want_hpss_metadata = request.headers.get('X-HPSS-Get-Metadata',
                                                         False)
                if config_true_value(want_hpss_metadata):
                    try:
                        hpss_headers = disk_file.read_hpss_system_metadata()
                    except SwiftOnFileSystemIOError:
                        return HTTPServiceUnavailable(request=request)
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
        response.last_modified = math.ceil(float(ts))
        # Needed for container sync feature
        response.headers['X-Timestamp'] = ts.normal
        response.headers['X-Backend-Timestamp'] = ts.internal
        response.content_length = int(metadata['Content-Length'])
        try:
            response.content_encoding = metadata['Content-Encoding']
        except KeyError:
            pass

        if hpss_headers:
            response.headers.update(hpss_headers)

        if 'X-Object-Sysmeta-Update-Container' in response.headers:
            self._sof_container_update(request, response)
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
            disk_file = self.get_diskfile(device, partition, account,
                                          container, obj, policy)
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
                    if disk_file.is_offline():
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
                want_hpss_metadata = request.headers.get('X-HPSS-Get-Metadata',
                                                         False)
                if config_true_value(want_hpss_metadata):
                    try:
                        hpss_headers = disk_file.read_hpss_system_metadata()
                        response.headers.update(hpss_headers)
                    except SwiftOnFileSystemIOError:
                        return HTTPServiceUnavailable(request=request)
                return request.get_response(response)
        except (DiskFileNotExist, DiskFileQuarantined) as e:
            headers = {}
            if hasattr(e, 'timestamp'):
                headers['X-Backend-Timestamp'] = e.timestamp.internal
            return HTTPNotFound(request=request, headers=headers,
                                conditional_response=True)

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
        purgelock = request.headers.get('X-HPSS-Purgelock-Status')
        if purgelock:
            try:
                hpssfs.ioctl(disk_file._fd, hpssfs.HPSSFS_PURGE_LOCK,
                             int(purgelock))
            except IOError as err:
                raise SwiftOnFileSystemIOError(
                    err.errno,
                    '%s, xattr.getxattr("%s", ...)' %
                    (err.strerror, disk_file._fd))

        # Set class of service if we got it
        cos = request.headers.get('X-HPSS-Class-Of-Service-ID')
        if cos:
            try:
                xattr.setxattr(disk_file._fd, 'system.hpss.cos', int(cos))
            except IOError as err:
                raise SwiftOnFileSystemIOError(
                    err.errno,
                    '%s, xattr.setxattr("%s", ...)' %
                    (err.strerror, disk_file._fd))

        # Update metadata from request
        try:
            orig_metadata = disk_file.read_metadata()
        except (DiskFileNotExist, DiskFileQuarantined):
            return HTTPNotFound(request=request)
        orig_timestamp = Timestamp(orig_metadata.get('X-Timestamp', 0))
        if orig_timestamp >= req_timestamp:
            backend_headers = {'X-Backend-Timestamp': orig_timestamp.internal}
            return HTTPConflict(request=request,
                                headers=backend_headers)
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
            with disk_file.open():
                orig_metadata = disk_file.read_metadata()
        except DiskFileXattrNotSupported:
            return HTTPInsufficientStorage(drive=device, request=request)
        except DiskFileExpired as e:
            orig_timestamp = e.timestamp
            orig_metadata = e.metadata
            response_class = HTTPNotFound
        except DiskFileDeleted as e:
            orig_timestamp = e.timestamp
            orig_metadata = {}
            response_class = HTTPNotFound
        # If the file got deleted outside of Swift, we won't see it.
        # So we say "file, what file?" and delete it from the container.
        except DiskFileNotExist:
            orig_timestamp = 0
            orig_metadata = {}
            response_class = HTTPNotFound
        except DiskFileQuarantined:
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
