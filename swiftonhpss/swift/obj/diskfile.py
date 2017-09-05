# Copyright (c) 2012-2013 Red Hat, Inc.
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

import os
import stat
import errno
try:
    from random import SystemRandom
    random = SystemRandom()
except ImportError:
    import random
import logging
import time
from hashlib import md5
from eventlet import sleep
from contextlib import contextmanager
from swiftonhpss.swift.common.exceptions import AlreadyExistsAsFile, \
    AlreadyExistsAsDir
from swift.common.utils import hash_path, \
    normalize_timestamp, Timestamp
from swift.common.exceptions import DiskFileNotExist, DiskFileError, \
    DiskFileNoSpace, DiskFileNotOpen, DiskFileExpired
from swift.common.swob import multi_range_iterator

from swiftonhpss.swift.common.exceptions import SwiftOnFileSystemOSError
from swiftonhpss.swift.common.fs_utils import do_fstat, do_open, do_close, \
    do_unlink, do_chown, do_fsync, do_stat, do_write, do_read, \
    do_rename, do_lseek, do_mkdir
from swiftonhpss.swift.common.utils import read_metadata, write_metadata,\
    rmobjdir, dir_is_object, \
    get_object_metadata, write_pickle, get_etag
from swiftonhpss.swift.common.utils import X_CONTENT_TYPE, \
    X_TIMESTAMP, X_TYPE, X_OBJECT_TYPE, FILE, OBJECT, DIR_TYPE, \
    FILE_TYPE, DEFAULT_UID, DEFAULT_GID, DIR_NON_OBJECT, DIR_OBJECT, \
    X_ETAG, X_CONTENT_LENGTH, X_MTIME
from swift.obj.diskfile import DiskFileManager as SwiftDiskFileManager
from swift.obj.diskfile import get_async_dir

import swiftonhpss.swift.common.hpss_utils as hpss_utils

import hpss.clapi as hpss

# FIXME: Hopefully we'll be able to move to Python 2.7+ where O_CLOEXEC will
# be back ported. See http://www.python.org/dev/peps/pep-0433/
O_CLOEXEC = 0o02000000

MAX_RENAME_ATTEMPTS = 10
MAX_OPEN_ATTEMPTS = 10


def _random_sleep():
    sleep(random.uniform(0.5, 0.15))


def make_directory(full_path, uid, gid, metadata=None):
    """
    Make a directory and change the owner ship as specified, and potentially
    creating the object metadata if requested.
    """
    try:
        do_mkdir(full_path)
    except OSError as err:
        if err.errno == errno.ENOENT:
            # Tell the caller some directory of the parent path does not
            # exist.
            return False, metadata
        elif err.errno == errno.EEXIST:
            # Possible race, in that the caller invoked this method when it
            # had previously determined the file did not exist.
            #
            # FIXME: When we are confident, remove this stat() call as it is
            # not necessary.
            try:
                stats = do_stat(full_path)
            except SwiftOnFileSystemOSError as serr:
                # FIXME: Ideally we'd want to return an appropriate error
                # message and code in the PUT Object REST API response.
                raise DiskFileError("make_directory: mkdir failed"
                                    " because path %s already exists, and"
                                    " a subsequent stat on that same"
                                    " path failed (%s)" % (full_path,
                                                           str(serr)))
            else:
                is_dir = stat.S_ISDIR(stats.st_mode)
                if not is_dir:
                    # FIXME: Ideally we'd want to return an appropriate error
                    # message and code in the PUT Object REST API response.
                    raise AlreadyExistsAsFile("make_directory:"
                                              " mkdir failed on path %s"
                                              " because it already exists"
                                              " but not as a directory"
                                              % (full_path))
            return True, metadata
        elif err.errno == errno.ENOTDIR:
            # FIXME: Ideally we'd want to return an appropriate error
            # message and code in the PUT Object REST API response.
            raise AlreadyExistsAsFile("make_directory:"
                                      " mkdir failed because some "
                                      "part of path %s is not in fact"
                                      " a directory" % (full_path))
        elif err.errno == errno.EIO:
            # Sometimes Fuse will return an EIO error when it does not know
            # how to handle an unexpected, but transient situation. It is
            # possible the directory now exists, stat() it to find out after a
            # short period of time.
            _random_sleep()
            try:
                stats = do_stat(full_path)
            except SwiftOnFileSystemOSError as serr:
                if serr.errno == errno.ENOENT:
                    errmsg = "make_directory: mkdir failed on" \
                             " path %s (EIO), and a subsequent stat on" \
                             " that same path did not find the file." % (
                                 full_path,)
                else:
                    errmsg = "make_directory: mkdir failed on" \
                             " path %s (%s), and a subsequent stat on" \
                             " that same path failed as well (%s)" % (
                                 full_path, str(err), str(serr))
                raise DiskFileError(errmsg)
            else:
                if not stats:
                    errmsg = "make_directory: mkdir failed on" \
                             " path %s (EIO), and a subsequent stat on" \
                             " that same path did not find the file." % (
                                 full_path,)
                    raise DiskFileError(errmsg)
                else:
                    # The directory at least exists now
                    is_dir = stat.S_ISDIR(stats.st_mode)
                    if is_dir:
                        # Dump the stats to the log with the original exception
                        logging.warn("make_directory: mkdir initially"
                                     " failed on path %s (%s) but a stat()"
                                     " following that succeeded: %r" %
                                     (full_path, str(err), stats))
                        # Assume another entity took care of the proper setup.
                        return True, metadata
                    else:
                        raise DiskFileError("make_directory: mkdir"
                                            " initially failed on path %s (%s)"
                                            " but now we see that it exists"
                                            " but is not a directory (%r)" %
                                            (full_path, str(err), stats))
        else:
            # Some other potentially rare exception occurred that does not
            # currently warrant a special log entry to help diagnose.
            raise DiskFileError("make_directory: mkdir failed on"
                                " path %s (%s)" % (full_path, str(err)))
    else:
        if metadata:
            # We were asked to set the initial metadata for this object.
            metadata_orig = get_object_metadata(full_path)
            metadata_orig.update(metadata)
            write_metadata(full_path, metadata_orig)
            metadata = metadata_orig

        # We created it, so we are reponsible for always setting the proper
        # ownership.
        do_chown(full_path, uid, gid)
        return True, metadata


def _adjust_metadata(fd, metadata):
    # Fix up the metadata to ensure it has a proper value for the
    # Content-Type metadata, as well as an X_TYPE and X_OBJECT_TYPE
    # metadata values.
    content_type = metadata.get(X_CONTENT_TYPE, '')

    if not content_type:
        # FIXME: How can this be that our caller supplied us with metadata
        # that has a content type that evaluates to False?
        #
        # FIXME: If the file exists, we would already know it is a
        # directory. So why are we assuming it is a file object?
        metadata[X_CONTENT_TYPE] = FILE_TYPE
        metadata[X_OBJECT_TYPE] = FILE
    else:
        if content_type.lower() == DIR_TYPE:
            metadata[X_OBJECT_TYPE] = DIR_OBJECT
        else:
            metadata[X_OBJECT_TYPE] = FILE

    # stat.st_mtime does not change after last write(). We set this to later
    # detect if the object was changed from filesystem interface (non Swift)
    statinfo = do_fstat(fd)
    if stat.S_ISREG(statinfo.st_mode):
        metadata[X_MTIME] = normalize_timestamp(statinfo.hpss_st_mtime)

    metadata[X_TYPE] = OBJECT
    return metadata


class DiskFileManager(SwiftDiskFileManager):
    """
    Management class for devices, providing common place for shared parameters
    and methods not provided by the DiskFile class (which primarily services
    the object server REST API layer).

    The `get_diskfile()` method is how this implementation creates a `DiskFile`
    object.

    .. note::

        This class is reference implementation specific and not part of the
        pluggable on-disk backend API.

    :param conf: caller provided configuration object
    :param logger: caller provided logger
    """
    def get_diskfile(self, device, partition, account, container, obj,
                     policy=None, **kwargs):
        hpss_dir_name = self.conf.get('hpss_swift_dir', '/swift')
        return DiskFile(self, hpss_dir_name,
                        partition, account, container, obj,
                        policy=policy, **kwargs)

    def pickle_async_update(self, device, account, container, obj, data,
                            timestamp, policy):
        # This should be using the JSON blob stuff instead of a pickle.
        # Didn't we deprecate it?
        device_path = self.construct_dev_path(device)
        async_dir = os.path.join(device_path, get_async_dir(policy))
        ohash = hash_path(account, container, obj)
        write_pickle(
            data,
            os.path.join(async_dir, ohash[-3:], ohash + '-' +
                         normalize_timestamp(timestamp)),
            os.path.join(device_path, 'tmp'))
        self.logger.increment('async_pendings')


class DiskFileWriter(object):
    """
    Encapsulation of the write context for servicing PUT REST API
    requests. Serves as the context manager object for DiskFile's create()
    method.


    """
    def __init__(self, fd, tmppath, disk_file):
        # Parameter tracking
        self._fd = fd
        self._tmppath = tmppath
        self._disk_file = disk_file

        # Internal attributes
        self._upload_size = 0
        self._last_sync = 0

        self._logger = self._disk_file._logger

    def _write_entire_chunk(self, chunk):
        bytes_per_sync = self._disk_file._mgr.bytes_per_sync
        while chunk:
            written = do_write(self._fd, chunk)
            chunk = chunk[written:]
            self._upload_size += written
            # For large files sync every 512MB (by default) written
            diff = self._upload_size - self._last_sync
            if diff >= bytes_per_sync:
                do_fsync(self._fd)
                self._last_sync = self._upload_size

    def close(self):
        """
        Close the file descriptor
        """
        if self._fd:
            do_close(self._fd)
            self._fd = None

    def write(self, chunk):
        """
        Write a chunk of data to disk.

        For this implementation, the data is written into a temporary file.

        :param chunk: the chunk of data to write as a string object

        :returns: the total number of bytes written to an object
        """
        self._write_entire_chunk(chunk)
        return self._upload_size

    def _finalize_put(self, metadata):
        # Write out metadata before fsync() to ensure it is also forced to
        # disk.
        write_metadata(self._tmppath, metadata)

        do_fsync(self._fd)

        self.set_checksum(metadata['ETag'])

        # At this point we know that the object's full directory path
        # exists, so we can just rename it directly without using Swift's
        # swift.common.utils.renamer(), which makes the directory path and
        # adds extra stat() calls.
        df = self._disk_file
        attempts = 1
        while True:
            try:
                do_rename(self._tmppath, df._data_file)
            except OSError as err:
                if err.errno in (errno.ENOENT, errno.EIO) \
                        and attempts < MAX_RENAME_ATTEMPTS:
                    # FIXME: Why either of these two error conditions is
                    # happening is unknown at this point. This might be a
                    # FUSE issue of some sort or a possible race
                    # condition. So let's sleep on it, and double check
                    # the environment after a good nap.
                    _random_sleep()
                    # Tease out why this error occurred. The man page for
                    # rename reads:
                    #   "The link named by tmppath does not exist; or, a
                    #    directory component in data_file does not exist;
                    #    or, tmppath or data_file is an empty string."
                    assert len(self._tmppath) > 0 and len(df._data_file) > 0
                    tpstats = do_stat(self._tmppath)
                    tfstats = do_fstat(self._fd)
                    assert tfstats
                    if not tpstats or tfstats.st_ino != tpstats.st_ino:
                        # Temporary file name conflict
                        raise DiskFileError(
                            'DiskFile.put(): temporary file, %s, was'
                            ' already renamed (targeted for %s)' % (
                                self._tmppath, df._data_file))
                    else:
                        # Data file target name now has a bad path!
                        dfstats = do_stat(df._put_datadir)
                        if not dfstats:
                            raise DiskFileError(
                                'DiskFile.put(): path to object, %s, no'
                                ' longer exists (targeted for %s)' % (
                                    df._put_datadir, df._data_file))
                        else:
                            is_dir = stat.S_ISDIR(dfstats.st_mode)
                            if not is_dir:
                                raise DiskFileError(
                                    'DiskFile.put(): path to object, %s,'
                                    ' no longer a directory (targeted for'
                                    ' %s)' % (self._put_datadir,
                                              df._data_file))
                            else:
                                # Let's retry since everything looks okay
                                logging.warn(
                                    "DiskFile.put(): rename('%s','%s')"
                                    " initially failed (%s) but a"
                                    " stat('%s') following that succeeded:"
                                    " %r" % (
                                        self._tmppath, df._data_file, str(err),
                                        df._put_datadir, dfstats))
                                attempts += 1
                                continue
                else:
                    raise SwiftOnFileSystemOSError(
                        err.errno, "%s, rename('%s', '%s')" % (
                            err.strerror, self._tmppath, df._data_file))
            else:
                # Success!
                break

        # Close here so the calling context does not have to perform this
        # in a thread.
        self.close()

    def put(self, metadata):
        """
        Finalize writing the file on disk, and renames it from the temp file
        to the real location.  This should be called after the data has been
        written to the temp file.

        :param purgelock: bool flag to signal if purge lock desired
        :param metadata: dictionary of metadata to be written
        :raises AlreadyExistsAsDir : If there exists a directory of the same
                                     name
        """
        assert self._tmppath is not None
        metadata = _adjust_metadata(self._fd, metadata)
        df = self._disk_file

        if dir_is_object(metadata):
            df._create_dir_object(df._data_file, metadata)
            return

        if df._is_dir:
            # A pre-existing directory already exists on the file
            # system, perhaps gratuitously created when another
            # object was created, or created externally to Swift
            # REST API servicing (UFO use case).
            raise AlreadyExistsAsDir('DiskFile.put(): file creation failed'
                                     ' since the target, %s, already exists'
                                     ' as a directory' % df._data_file)

        self._finalize_put(metadata)

        # Avoid the unlink() system call as part of the mkstemp context
        # cleanup
        self._tmppath = None

    def commit(self, timestamp):
        """
        Perform any operations necessary to mark the object as durable. For
        replication policy type this is a no-op.

        :param timestamp: object put timestamp, an instance of
                          :class:`~swift.common.utils.Timestamp`
        """
        pass

    def set_checksum(self, checksum):
        hpss_utils.set_checksum(self._fd, checksum)


class DiskFileReader(object):
    """
    Encapsulation of the WSGI read context for servicing GET REST API
    requests. Serves as the context manager object for the
    :class:`swift.obj.diskfile.DiskFile` class's
    :func:`swift.obj.diskfile.DiskFile.reader` method.

    .. note::

        The quarantining behavior of this method is considered implementation
        specific, and is not required of the API.

    .. note::

        The arguments to the constructor are considered implementation
        specific. The API does not define the constructor arguments.

    :param fp: open file descriptor, -1 for a directory object
    :param disk_chunk_size: size of reads from disk in bytes
    :param obj_size: size of object on disk
    :param keep_cache_size: maximum object size that will be kept in cache
    :param iter_hook: called when __iter__ returns a chunk
    :param keep_cache: should resulting reads be kept in the buffer cache
    """
    def __init__(self, fd, disk_chunk_size, obj_size,
                 keep_cache_size, iter_hook=None, keep_cache=False):
        # Parameter tracking
        self._fd = fd
        self._disk_chunk_size = disk_chunk_size
        self._iter_hook = iter_hook
        if keep_cache:
            # Caller suggests we keep this in cache, only do it if the
            # object's size is less than the maximum.
            self._keep_cache = obj_size < keep_cache_size
        else:
            self._keep_cache = False

        # Internal Attributes
        self._suppress_file_closing = False

    def __iter__(self):
        """Returns an iterator over the data file."""
        try:
            dropped_cache = 0
            bytes_read = 0
            while True:
                if self._fd != -1:
                    chunk = do_read(self._fd, self._disk_chunk_size)
                else:
                    chunk = None
                if chunk:
                    bytes_read += len(chunk)
                    diff = bytes_read - dropped_cache
                    if diff > (1024 * 1024):
                        self._drop_cache(dropped_cache, diff)
                        dropped_cache = bytes_read
                    yield chunk
                    if self._iter_hook:
                        self._iter_hook()
                else:
                    diff = bytes_read - dropped_cache
                    if diff > 0:
                        self._drop_cache(dropped_cache, diff)
                    break
        finally:
            if not self._suppress_file_closing:
                self.close()

    def app_iter_range(self, start, stop):
        """Returns an iterator over the data file for range (start, stop)"""
        if start or start == 0:
            do_lseek(self._fd, start, os.SEEK_SET)
        if stop is not None:
            length = stop - start
        else:
            length = None
        try:
            for chunk in self:
                if length is not None:
                    length -= len(chunk)
                    if length < 0:
                        # Chop off the extra:
                        yield chunk[:length]
                        break
                yield chunk
        finally:
            if not self._suppress_file_closing:
                self.close()

    def app_iter_ranges(self, ranges, content_type, boundary, size):
        """Returns an iterator over the data file for a set of ranges"""
        if not ranges:
            yield ''
        else:
            try:
                self._suppress_file_closing = True
                for chunk in multi_range_iterator(
                        ranges, content_type, boundary, size,
                        self.app_iter_range):
                    yield chunk
            finally:
                self._suppress_file_closing = False
                self.close()

    def _drop_cache(self, offset, length):
        pass

    def close(self):
        """
        Close the open file handle if present.
        """
        if self._fd is not None:
            fd, self._fd = self._fd, None
            if fd > -1:
                do_close(fd)


class DiskFile(object):
    """
    Manage object files on disk.

    Object names ending or beginning with a '/' as in /a, a/, /a/b/,
    etc, or object names with multiple consecutive slashes, like a//b,
    are not supported.  The proxy server's constraints filter
    swiftonhpss.common.constrains.check_object_creation() should
    reject such requests.

    :param mgr: associated on-disk manager instance
    :param dev_path: device name/account_name for UFO.
    :param account: account name for the object
    :param container: container name for the object
    :param obj: object name for the object
    :param uid: user ID disk object should assume (file or directory)
    :param gid: group ID disk object should assume (file or directory)
    """
    def __init__(self, mgr, dev_path, partition,
                 account=None, container=None, obj=None,
                 policy=None, uid=DEFAULT_UID, gid=DEFAULT_GID, **kwargs):
        # Variables partition and policy is currently unused.
        self._mgr = mgr
        self._logger = mgr.logger
        self._device_path = dev_path
        self._uid = uid
        self._gid = gid
        self._is_dir = False
        self._metadata = None
        self._fd = -1
        # Save stat info as internal variable to avoid multiple stat() calls
        self._stat = None
        # Save md5sum of object as internal variable to avoid reading the
        # entire object more than once.
        self._etag = None
        self._file_has_changed = None
        # Don't store a value for data_file until we know it exists.
        self._data_file = None

        self._obj_size = 0

        # Account name contains reseller_prefix which is retained and not
        # stripped. This to conform to Swift's behavior where account name
        # entry in Account DBs contain reseller_prefix.
        self._account = account
        self._container = container

        self._container_path = \
            os.path.join(self._device_path, self._account, self._container)
        obj = obj.strip(os.path.sep)
        obj_path, self._obj = os.path.split(obj)
        if obj_path:
            self._obj_path = obj_path.strip(os.path.sep)
            self._put_datadir = os.path.join(self._container_path,
                                             self._obj_path)
        else:
            self._obj_path = ''
            self._put_datadir = self._container_path

        self._data_file = os.path.join(self._put_datadir, self._obj)

    @property
    def timestamp(self):
        if self._metadata is None:
            raise DiskFileNotOpen()
        return Timestamp(self._metadata.get('X-Timestamp'))

    @property
    def data_timestamp(self):
        if self._metadata is None:
            raise DiskFileNotOpen()
        return Timestamp(self._metadata.get('X-Timestamp'))

    def open(self):
        """
        Open the object.

        This implementation opens the data file representing the object, reads
        the associated metadata in the extended attributes, additionally
        combining metadata from fast-POST `.meta` files.

        .. note::

            An implementation is allowed to raise any of the following
            exceptions, but is only required to raise `DiskFileNotExist` when
            the object representation does not exist.

        :raises DiskFileNotExist: if the object does not exist
        :raises DiskFileExpired: if the object has expired
        :returns: itself for use as a context manager
        """
        # Writes are always performed to a temporary file
        try:
            self._logger.debug("DiskFile: Opening %s" % self._data_file)
            self._stat = do_stat(self._data_file)
            self._is_dir = stat.S_ISDIR(self._stat.st_mode)
            if not self._is_dir:
                self._fd = do_open(self._data_file, hpss.O_RDONLY)
                self._obj_size = self._stat.st_size
            else:
                self._fd = -1
                self._obj_size = 0
        except SwiftOnFileSystemOSError as err:
            if err.errno in (errno.ENOENT, errno.ENOTDIR):
                # If the file does exist, or some part of the path does not
                # exist, raise the expected DiskFileNotExist
                raise DiskFileNotExist
            raise
        try:
            self._logger.debug("DiskFile: Reading metadata")
            self._metadata = read_metadata(self._data_file)
            if not self._validate_object_metadata():
                self._create_object_metadata(self._data_file)

            assert self._metadata is not None
            self._filter_metadata()

            if not self._is_dir:
                if self._is_object_expired(self._metadata):
                    raise DiskFileExpired(metadata=self._metadata)

        except (OSError, IOError, DiskFileExpired) as err:
            # Something went wrong. Context manager will not call
            # __exit__. So we close the fd manually here.
            self._close_fd()
            if hasattr(err, 'errno') and err.errno == errno.ENOENT:
                # Handle races: ENOENT can be raised by read_metadata()
                # call in GlusterFS if file gets deleted by another
                # client after do_open() succeeds
                logging.warn("open(%s) succeeded but one of the subsequent "
                             "syscalls failed with ENOENT. Raising "
                             "DiskFileNotExist." % (self._data_file))
                raise DiskFileNotExist
            else:
                # Re-raise the original exception after fd has been closed
                raise

        return self

    def _validate_object_metadata(self):
        # Has no Swift specific metadata saved as xattr. Probably because
        # object was added/replaced through filesystem interface.
        if not self._metadata and not self._is_dir:
            self._file_has_changed = True
            return False

        required_keys = \
            (X_TIMESTAMP, X_CONTENT_TYPE, X_CONTENT_LENGTH, X_ETAG,
             # SOF specific keys
             X_TYPE, X_OBJECT_TYPE)

        if not all(k in self._metadata for k in required_keys):
            # At least one of the required keys does not exist
            return False

        if not self._is_dir:
            # X_MTIME is a new key added recently, newer objects will
            # have the key set during PUT.
            if X_MTIME in self._metadata:
                # Check if the file has been modified through filesystem
                # interface by comparing mtime stored in xattr during PUT
                # and current mtime of file.
                obj_stat = do_stat(self._data_file)
                if normalize_timestamp(self._metadata[X_MTIME]) != \
                        normalize_timestamp(obj_stat.hpss_st_mtime):
                    self._file_has_changed = True
                    return False
            else:
                # Without X_MTIME key, comparing md5sum is the only way
                # to determine if file has changed or not. This is inefficient
                # but there's no other way!
                self._etag = get_etag(self._data_file)
                if self._etag != self._metadata[X_ETAG]:
                    self._file_has_changed = True
                    return False
                else:
                    # Checksums are same; File has not changed. For the next
                    # GET request on same file, we don't compute md5sum again!
                    # This is achieved by setting X_MTIME to mtime in
                    # _create_object_metadata()
                    return False

        if self._metadata[X_TYPE] == OBJECT:
            return True

        return False

    def _create_object_metadata(self, file_path):
        if self._etag is None:
            self._etag = md5().hexdigest() if self._is_dir \
                else get_etag(file_path)

        if self._file_has_changed or (X_TIMESTAMP not in self._metadata):
            timestamp = normalize_timestamp(self._stat.st_mtime)
        else:
            timestamp = self._metadata[X_TIMESTAMP]

        metadata = {
            X_TYPE: OBJECT,
            X_TIMESTAMP: timestamp,
            X_CONTENT_TYPE: DIR_TYPE if self._is_dir else FILE_TYPE,
            X_OBJECT_TYPE: DIR_NON_OBJECT if self._is_dir else FILE,
            X_CONTENT_LENGTH: 0 if self._is_dir else self._stat.st_size,
            X_ETAG: self._etag}

        # Add X_MTIME key if object is a file
        if not self._is_dir:
            metadata[X_MTIME] = normalize_timestamp(self._stat.hpss_st_mtime)

        meta_new = self._metadata.copy()
        meta_new.update(metadata)
        if self._metadata != meta_new:
            write_metadata(file_path, meta_new)
            # Avoid additional read_metadata() later
            self._metadata = meta_new

    def _filter_metadata(self):
        for key in (X_TYPE, X_OBJECT_TYPE, X_MTIME):
            self._metadata.pop(key, None)
        if self._file_has_changed:
            # Really ugly hack to let SOF's GET() wrapper know that we need
            # to update the container database
            self._metadata['X-Object-Sysmeta-Update-Container'] = True

    def _is_object_expired(self, metadata):
        try:
            x_delete_at = int(metadata['X-Delete-At'])
        except KeyError:
            pass
        except ValueError:
            # x-delete-at key is present but not an integer.
            # TODO: Openstack Swift "quarantines" the object.
            # We just let it pass
            pass
        else:
            if x_delete_at <= time.time():
                return True
        return False

    # TODO: don't parse this, just grab data structure
    def is_offline(self):
        meta = hpss_utils.read_hpss_system_metadata(self._data_file)
        raw_file_levels = meta['X-HPSS-Data-Levels']
        file_levels = raw_file_levels.split(";")
        top_level = file_levels[0].split(':')
        bytes_on_disk = top_level[2].rstrip(' ')
        if bytes_on_disk == 'nodata':
            bytes_on_disk = '0'
        return int(bytes_on_disk) != self._stat.st_size

    def __enter__(self):
        """
        Context enter.

        .. note::

            An implementation shall raise `DiskFileNotOpen` when has not
            previously invoked the :func:`swift.obj.diskfile.DiskFile.open`
            method.
        """
        if self._metadata is None:
            raise DiskFileNotOpen()
        return self

    def _close_fd(self):
        if self._fd is not None:
            fd, self._fd = self._fd, None
            if fd > -1:
                do_close(fd)

    def __exit__(self, t, v, tb):
        """
        Context exit.

        .. note::

            This method will be invoked by the object server while servicing
            the REST API *before* the object has actually been read. It is the
            responsibility of the implementation to properly handle that.
        """
        self._close_fd()

    def get_metadata(self):
        """
        Provide the metadata for a previously opened object as a dictionary.

        :returns: object's metadata dictionary
        :raises DiskFileNotOpen: if the
            :func:`swift.obj.diskfile.DiskFile.open` method was not previously
            invoked
        """
        if self._metadata is None:
            raise DiskFileNotOpen()
        return self._metadata

    def read_metadata(self):
        """
        Return the metadata for an object without opening the object's file on
        disk.

        :returns: metadata dictionary for an object
        :raises DiskFileError: this implementation will raise the same
                            errors as the `open()` method.
        """
        # FIXME: pull a lot of this and the copy of it from open() out to
        # another function

        # Do not actually open the file, in order to duck hpssfs checksum
        # validation and resulting timeouts
        # This means we do a few things DiskFile.open() does.
        try:
            if not self._stat:
                self._stat = do_stat(self._data_file)
            self._is_dir = stat.S_ISDIR(self._stat.st_mode)
            self._metadata = read_metadata(self._data_file)
        except SwiftOnFileSystemOSError:
            raise DiskFileNotExist
        if not self._validate_object_metadata():
            self._create_object_metadata(self._data_file)
        self._filter_metadata()
        return self._metadata

    def read_hpss_system_metadata(self):
        return hpss_utils.read_hpss_system_metadata(self._data_file)

    def reader(self, iter_hook=None, keep_cache=False):
        """
        Return a :class:`swift.common.swob.Response` class compatible
        "`app_iter`" object as defined by
        :class:`swift.obj.diskfile.DiskFileReader`.

        For this implementation, the responsibility of closing the open file
        is passed to the :class:`swift.obj.diskfile.DiskFileReader` object.

        :param iter_hook: called when __iter__ returns a chunk
        :param keep_cache: caller's preference for keeping data read in the
                           OS buffer cache
        :returns: a :class:`swift.obj.diskfile.DiskFileReader` object
        """
        if self._metadata is None:
            raise DiskFileNotOpen()
        dr = DiskFileReader(
            self._fd, self._mgr.disk_chunk_size,
            self._obj_size, self._mgr.keep_cache_size,
            iter_hook=iter_hook, keep_cache=keep_cache)
        # At this point the reader object is now responsible for closing
        # the file pointer.
        self._fd = None
        return dr

    def _create_dir_object(self, dir_path, metadata=None):
        """
        Create a directory object at the specified path. No check is made to
        see if the directory object already exists, that is left to the caller
        (this avoids a potentially duplicate stat() system call).

        The "dir_path" must be relative to its container, self._container_path.

        The "metadata" object is an optional set of metadata to apply to the
        newly created directory object. If not present, no initial metadata is
        applied.

        The algorithm used is as follows:

          1. An attempt is made to create the directory, assuming the parent
             directory already exists

             * Directory creation races are detected, returning success in
               those cases

          2. If the directory creation fails because some part of the path to
             the directory does not exist, then a search back up the path is
             performed to find the first existing ancestor directory, and then
             the missing parents are successively created, finally creating
             the target directory
        """
        full_path = os.path.join(self._container_path, dir_path)
        cur_path = full_path
        stack = []
        while True:
            md = None if cur_path != full_path else metadata
            ret, newmd = make_directory(cur_path, self._uid, self._gid, md)
            if ret:
                break
            # Some path of the parent did not exist, so loop around and
            # create that, pushing this parent on the stack.
            if os.path.sep not in cur_path:
                raise DiskFileError("DiskFile._create_dir_object(): failed to"
                                    " create directory path while exhausting"
                                    " path elements to create: %s" % full_path)
            cur_path, child = cur_path.rsplit(os.path.sep, 1)
            assert child
            stack.append(child)

        child = stack.pop() if stack else None
        while child:
            cur_path = os.path.join(cur_path, child)
            md = None if cur_path != full_path else metadata
            ret, newmd = make_directory(cur_path, self._uid, self._gid, md)
            if not ret:
                raise DiskFileError("DiskFile._create_dir_object(): failed to"
                                    " create directory path to target, %s,"
                                    " on subpath: %s" % (full_path, cur_path))
            child = stack.pop() if stack else None
        return True, newmd

    @contextmanager
    def create(self, hpss_hints):
        """
        Context manager to create a file. We create a temporary file first, and
        then return a DiskFileWriter object to encapsulate the state.

        For Gluster, we first optimistically create the temporary file using
        the "rsync-friendly" .NAME.random naming. If we find that some path to
        the file does not exist, we then create that path and then create the
        temporary file again. If we get file name conflict, we'll retry using
        different random suffixes 1,000 times before giving up.

        .. note::

            An implementation is not required to perform on-disk
            preallocations even if the parameter is specified. But if it does
            and it fails, it must raise a `DiskFileNoSpace` exception.

        :param hpss_hints: dict containing HPSS class of service hints
        :raises DiskFileNoSpace: if a size is specified and allocation fails
        :raises AlreadyExistsAsFile: if path or part of a path is not a \
                                     directory
        """
        # Create /account/container directory structure on mount point root
        try:
            self._logger.debug("DiskFile: Creating directories for %s" %
                               self._container_path)
            hpss_utils.makedirs(self._container_path)
        except OSError as err:
            if err.errno != errno.EEXIST:
                raise

        # Create directories to object name, if they don't exist.
        self._logger.debug("DiskFile: creating directories in obj name %s" %
                           self._obj)
        object_dirs = os.path.split(self._obj)[0]
        self._logger.debug("object_dirs: %s" % repr(object_dirs))
        self._logger.debug("data_dir: %s" % self._put_datadir)
        hpss_utils.makedirs(os.path.join(self._put_datadir, object_dirs))

        data_file = os.path.join(self._put_datadir, self._obj)

        # Create the temporary file
        attempts = 1
        while True:
            tmpfile = '.%s.%s.temporary' % (self._obj,
                                            random.randint(0, 65536))
            tmppath = os.path.join(self._put_datadir, tmpfile)
            self._logger.debug("DiskFile: Creating temporary file %s" %
                               tmppath)

            try:
                # TODO: figure out how to create hints struct
                self._logger.debug("DiskFile: creating file")
                fd = do_open(tmppath,
                             hpss.O_WRONLY | hpss.O_CREAT | hpss.O_EXCL)
                self._logger.debug("DiskFile: setting COS")
                if hpss_hints['cos']:
                    hpss_utils.set_cos(tmppath, int(hpss_hints['cos']))

            except SwiftOnFileSystemOSError as gerr:
                if gerr.errno in (errno.ENOSPC, errno.EDQUOT):
                    # Raise DiskFileNoSpace to be handled by upper layers when
                    # there is no space on disk OR when quota is exceeded
                    self._logger.error("DiskFile: no space")
                    raise DiskFileNoSpace()
                if gerr.errno == errno.EACCES:
                    self._logger.error("DiskFile: permission denied")
                    raise DiskFileNoSpace()
                if gerr.errno == errno.ENOTDIR:
                    self._logger.error("DiskFile: not a directory")
                    raise AlreadyExistsAsFile('do_open(): failed on %s,'
                                              '  path or part of a'
                                              ' path is not a directory'
                                              % data_file)

                if gerr.errno not in (errno.ENOENT, errno.EEXIST, errno.EIO):
                    # FIXME: Other cases we should handle?
                    self._logger.error("DiskFile: unknown error %s"
                                       % gerr.errno)
                    raise
                if attempts >= MAX_OPEN_ATTEMPTS:
                    # We failed after N attempts to create the temporary
                    # file.
                    raise DiskFileError('DiskFile.mkstemp(): failed to'
                                        ' successfully create a temporary file'
                                        ' without running into a name conflict'
                                        ' after %d of %d attempts for: %s' % (
                                            attempts, MAX_OPEN_ATTEMPTS,
                                            data_file))
                if gerr.errno == errno.EEXIST:
                    self._logger.debug("DiskFile: file exists already")
                    # Retry with a different random number.
                    attempts += 1
            else:
                break
        dw = None

        self._logger.debug("DiskFile: created file")

        try:
            # Ensure it is properly owned before we make it available.
            do_chown(tmppath, self._uid, self._gid)
            dw = DiskFileWriter(fd, tmppath, self)
            yield dw
        finally:
            if dw:
                dw.close()
                if dw._tmppath:
                    do_unlink(dw._tmppath)

    def write_metadata(self, metadata):
        """
        Write a block of metadata to an object without requiring the caller to
        open the object first.

        :param metadata: dictionary of metadata to be associated with the
                         object
        :raises DiskFileError: this implementation will raise the same
                            errors as the `create()` method.
        """
        metadata = self._keep_sys_metadata(metadata)
        data_file = os.path.join(self._put_datadir, self._obj)
        write_metadata(data_file, metadata)

    def _keep_sys_metadata(self, metadata):
        """
        Make sure system metadata is not lost when writing new user metadata

        This method will read the existing metadata and check for system
        metadata. If there are any, it should be appended to the metadata obj
        the user is trying to write.
        """
        orig_metadata = self.read_metadata()

        sys_keys = [X_CONTENT_TYPE, X_ETAG, 'name', X_CONTENT_LENGTH,
                    X_OBJECT_TYPE, X_TYPE]

        for key in sys_keys:
            if key in orig_metadata:
                metadata[key] = orig_metadata[key]

        if X_OBJECT_TYPE not in orig_metadata:
            if metadata[X_CONTENT_TYPE].lower() == DIR_TYPE:
                metadata[X_OBJECT_TYPE] = DIR_OBJECT
            else:
                metadata[X_OBJECT_TYPE] = FILE

        if X_TYPE not in orig_metadata:
            metadata[X_TYPE] = OBJECT

        return metadata

    def _unlinkold(self):
        if self._is_dir:
            # Marker, or object, directory.
            #
            # Delete from the filesystem only if it contains no objects.
            # If it does contain objects, then just remove the object
            # metadata tag which will make this directory a
            # fake-filesystem-only directory and will be deleted when the
            # container or parent directory is deleted.
            #
            # FIXME: Ideally we should use an atomic metadata update operation
            metadata = read_metadata(self._data_file)
            if dir_is_object(metadata):
                metadata[X_OBJECT_TYPE] = DIR_NON_OBJECT
                write_metadata(self._data_file, metadata)
            rmobjdir(self._data_file)
        else:
            # Delete file object
            do_unlink(self._data_file)

        # Garbage collection of non-object directories.  Now that we
        # deleted the file, determine if the current directory and any
        # parent directory may be deleted.
        dirname = os.path.dirname(self._data_file)
        while dirname and dirname != self._container_path:
            # Try to remove any directories that are not objects.
            if not rmobjdir(dirname):
                # If a directory with objects has been found, we can stop
                # garabe collection
                break
            else:
                dirname = os.path.dirname(dirname)

    def set_cos(self, cos_id):
        hpss_utils.set_cos(self._data_file, cos_id)

    def set_purge_lock(self, purgelock):
        hpss_utils.set_purge_lock(self._data_file, purgelock)

    def delete(self, timestamp):
        """
        Delete the object.

        This implementation creates a tombstone file using the given
        timestamp, and removes any older versions of the object file. Any
        file that has an older timestamp than timestamp will be deleted.

        .. note::

            An implementation is free to use or ignore the timestamp
            parameter.

        :param timestamp: timestamp to compare with each file
        :raises DiskFileError: this implementation will raise the same
                            errors as the `create()` method.
        """
        try:
            metadata = read_metadata(self._data_file)
        except (IOError, OSError) as err:
            if err.errno != errno.ENOENT:
                raise
        else:
            if metadata[X_TIMESTAMP] >= timestamp:
                return

        self._unlinkold()

        self._metadata = None
        self._data_file = None
