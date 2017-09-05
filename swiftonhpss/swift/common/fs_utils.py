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

import logging
import os
import errno
import time
from collections import defaultdict
from itertools import repeat
from swiftonhpss.swift.common.exceptions import SwiftOnFileSystemOSError
from swift.common.exceptions import DiskFileNoSpace

import hpss.clapi as hpss
import hpss_utils


def do_getxattr(path, key):
    return hpss_utils.read_uda(path, key)


def do_setxattr(path, key, value):
    return hpss_utils.write_uda(path, key, value)


def do_removexattr(path, key):
    return hpss_utils.delete_uda(path, key)


def do_walk(*args, **kwargs):
    return hpss_utils.walk(*args, **kwargs)


def do_write(fd, buf):
    try:
        cnt = hpss.Write(fd, buf)
    except IOError as err:
        if err.errno in (errno.ENOSPC, errno.EDQUOT):
            raise DiskFileNoSpace()
        else:
            raise SwiftOnFileSystemOSError(
                err.errno, '%s, os.write("%s", ...)' % (err.strerror, fd))
    return cnt


def do_read(fd, n):
    try:
        buf = hpss.Read(fd, n)
    except IOError as err:
        raise SwiftOnFileSystemOSError(
            err.errno, '%s, os.read("%s", ...)' % (err.strerror, fd))
    return buf


def do_mkdir(path):
    hpss.Mkdir(path, 0o600)


def do_rmdir(path):
    try:
        hpss.Rmdir(path)
    except IOError as err:
        raise SwiftOnFileSystemOSError(
            err.errno, '%s, os.rmdir("%s")' % (err.strerror, path))


def do_chown(path, uid, gid):
    try:
        hpss.Chown(path, uid, gid)
    except IOError as err:
        raise SwiftOnFileSystemOSError(
            err.errno, '%s, os.chown("%s", %s, %s)' % (
                err.strerror, path, uid, gid))


def do_fchown(fd, uid, gid):
    try:
        # TODO: grab path name from fd, chown that
        os.fchown(fd, uid, gid)
    except IOError as err:
        raise SwiftOnFileSystemOSError(
            err.errno, '%s, os.fchown(%s, %s, %s)' % (
                err.strerror, fd, uid, gid))


_STAT_ATTEMPTS = 10


def do_stat(path):
    try:
        stats = hpss.Stat(path)
    except IOError as err:
        raise SwiftOnFileSystemOSError(
            err.errno, '%s, os.stat(%s)' % (err.strerror, path))
    return stats


def do_fstat(fd):
    try:
        stats = hpss.Fstat(fd)
    except IOError as err:
        raise SwiftOnFileSystemOSError(
            err.errno, '%s, os.fstat(%s)' % (err.strerror, fd))
    return stats


def do_open(path, flags, mode=0o777, hints=None):
    hints_struct = hpss.cos_hints()
    try:
        if hints:
            cos = hints.get('cos', '0')
            hints_struct.cos = int(cos)
            file_handle = hpss.Open(path, flags, mode, hints_struct)
        else:
            file_handle = hpss.Open(path, flags, mode)
        fd = file_handle[0]
    except IOError as err:
        raise SwiftOnFileSystemOSError(
            err.errno, '%s, os.open("%s", %x, %o)' % (
                err.strerror, path, flags, mode))
    return fd


def do_close(fd):
    try:
        hpss.Close(fd)
    except IOError as err:
        if err.errno in (errno.ENOSPC, errno.EDQUOT):
            raise DiskFileNoSpace()
        else:
            raise SwiftOnFileSystemOSError(
                err.errno, '%s, os.close(%s)' % (err.strerror, fd))


def do_unlink(path, log=True):
    try:
        hpss.Unlink(path)
    except IOError as err:
        if err.errno != errno.ENOENT:
            raise SwiftOnFileSystemOSError(
                err.errno, '%s, os.unlink("%s")' % (err.strerror, path))
        else:
            logging.warn("fs_utils: os.unlink failed on non-existent path: %s",
                         path)


def do_rename(old_path, new_path):
    try:
        hpss.Rename(old_path, new_path)
    except IOError as err:
        raise SwiftOnFileSystemOSError(
            err.errno, '%s, os.rename("%s", "%s")' % (
                err.strerror, old_path, new_path))


def do_fsync(fd):
    try:
        hpss.Fsync(fd)
    except IOError as err:
        raise SwiftOnFileSystemOSError(
            err.errno, '%s, os.fsync("%s")' % (err.strerror, fd))


def do_lseek(fd, pos, how):
    try:
        hpss.Lseek(fd, pos, how)
    except IOError as err:
        raise SwiftOnFileSystemOSError(
            err.errno, '%s, os.lseek("%s")' % (err.strerror, fd))


def static_var(varname, value):
    """Decorator function to create pseudo static variables."""
    def decorate(func):
        setattr(func, varname, value)
        return func
    return decorate

# Rate limit to emit log message once a second
_DO_LOG_RL_INTERVAL = 1.0


@static_var("counter", defaultdict(int))
@static_var("last_called", defaultdict(repeat(0.0).next))
def do_log_rl(msg, *args, **kwargs):
    """
    Rate limited logger.

    :param msg: String or message to be logged
    :param log_level: Possible values- error, warning, info, debug, critical
    """
    log_level = kwargs.get('log_level', "error")
    if log_level not in ("error", "warning", "info", "debug", "critical"):
        log_level = "error"

    do_log_rl.counter[msg] += 1  # Increment msg counter
    interval = time.time() - do_log_rl.last_called[msg]

    if interval >= _DO_LOG_RL_INTERVAL:
        # Prefix PID of process and message count to original log msg
        emit_msg = "[PID:" + str(os.getpid()) + "]" \
            + "[RateLimitedLog;Count:" + str(do_log_rl.counter[msg]) + "] " \
            + msg
        # log_level is a param for do_log_rl and not for logging.* methods
        try:
            del kwargs['log_level']
        except KeyError:
            pass

        getattr(logging, log_level)(emit_msg, *args, **kwargs)  # Emit msg
        do_log_rl.counter[msg] = 0  # Reset msg counter when message is emitted
        do_log_rl.last_called[msg] = time.time()  # Reset msg time
