"""Utility classes and methods for the redisbus internals and users"""

import time
import sys
import subprocess
import platform
import os
import zipfile
import tempfile
import base64
import shutil
import logging
import random


ISO_STRFTIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'


class DictObj(object):
    """Convert a dictionary into a python object"""
    def __init__(self, d):
        self.d = d
        for key, value in d.items():
            if isinstance(value, (list, tuple)):
                setattr(self, key, [DictObj(element)
                        if isinstance(element, dict) else element for element in value])
            else:
                setattr(self, key, DictObj(value)
                        if isinstance(value, dict) else value)

    def __repr__(self):
        return ', '.join(('{}: {}'.format(key, repr(value))
                          for (key, value) in self.__dict__.items()))


class Periodic(object):
    """Periodically yield a positive return value"""
    def __init__(self, interval, jitter=0):
        self.interval = interval
        self.jitter = jitter
        self.last = 0

    def set(self):
        self.last = time.time()

    def check(self):
        now = time.time()
        if now - self.last >= self.interval:
            self.last = now + random.uniform(0, self.jitter)
            return True
        return False


CREATE_NEW_PROCESS_GROUP = 0x00000200
DETACHED_PROCESS = 0x00000008


def launch_detached(cmd):
    """
    Launch a new process in a detached state, the process will not block if IO is not read
    """
    kwargs = {}
    if platform.system() == 'Windows':
        kwargs.update(creationflags=DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP)
    elif sys.version_info < (3, 2):
        kwargs.update(preexec_fn=os.setsid)
    else:
        kwargs.update(start_new_session=True)

    return subprocess.Popen(cmd, bufsize=-1, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            **kwargs)


try:
    from subprocess import DEVNULL  # Python 3.
except ImportError:
    DEVNULL = open(os.devnull, 'wb')


def launch_nopipe(cmd):
    """
    Launch a new process with stdout and stderr detached
    """
    return subprocess.Popen(cmd, stdout=DEVNULL, stderr=DEVNULL)


def add_files_to_zip(zout, path, log):
    """Add a set of files to a zip file compression stream"""
    for root, dirs, files in os.walk(path):
        for file_name in files:
            full_path = os.path.join(root, file_name)
            zout.write(full_path)
            log.info('added {} to archive'.format(full_path))


def compress_and_chunk_file(path, chunk_size=2048, use_base64=True, log=None):
    """Compress a file yielding the compressed chunks of the file"""
    name = os.path.split(path)[-1]
    dest_path = tempfile.mkdtemp()
    zip_name = name + '.zip'
    full_zip_path = os.path.join(dest_path, zip_name)
    with zipfile.ZipFile(full_zip_path, mode='w', compression=zipfile.ZIP_STORED, allowZip64=True) as zout:
        zout.debug = 3
        log.info('created archive {}'.format(zip_name))
        if os.path.isdir(path):
            add_files_to_zip(zout, path, log)
        else:
            zout.write(path)
            log.info('added {} to archive'.format(path))

    with open(full_zip_path, 'rb') as reader:
        while True:
            chunk = reader.read(chunk_size)
            if not chunk:
                break
            if use_base64:
                yield base64.b64encode(chunk)
            else:
                yield chunk

    shutil.rmtree(dest_path, ignore_errors=True)


def chunk_file(path, chunk_size=2048, use_base64=True):
    """Yield chunk_size chunks of file at path, optionally base64 encode the chunks"""
    with open(path, 'rb') as reader:
        while True:
            chunk_data = reader.read(chunk_size)
            if not chunk_data:
                break
            if use_base64:
                yield base64.b64encode(chunk_data)
            else:
                yield chunk_data


def log_formatter():
    """Return a standard logging formatter"""
    return logging.Formatter('[%(levelname)s %(asctime)s] %(name)s: %(message)s (%(pathname)s:%(lineno)s)')


INTERVALS = [1, 60, 3600, 86400, 604800, 2419200, 29030400]
NAMES = [('second', 'seconds'),
         ('minute', 'minutes'),
         ('hour', 'hours'),
         ('day', 'days'),
         ('week', 'weeks'),
         ('month', 'months'),
         ('year', 'years')]


def humanize_time(amount, units):
    """
    Divide `amount` in time periods.
    Useful for making time intervals more human readable.

    >>> humanize_time(173, 'hours')
    [(1, 'week'), (5, 'hours')]
    >>> humanize_time(17313, 'seconds')
    [(4, 'hours'), (48, 'minutes'), (33, 'seconds')]
    >>> humanize_time(90, 'weeks')
    [(1, 'year'), (10, 'months'), (2, 'weeks')]
    >>> humanize_time(42, 'months')
    [(3, 'years'), (6, 'months')]
    >>> humanize_time(500, 'days')
    [(1, 'year'), (5, 'months'), (3, 'weeks'), (3, 'days')]
    """
    result = []
    unit = list(map(lambda t: t[1], NAMES)).index(units)
    # Convert to seconds
    amount = amount * INTERVALS[unit]
    for i in range(len(NAMES)-1, -1, -1):
        a = amount // INTERVALS[i]
        if a > 0:
            result.append((a, NAMES[i][1 % a]))
            amount -= a * INTERVALS[i]
    return result


def humanize_time_str(amount, units):
    """Returns simple string version of humanize_time"""
    tokens = []
    for token in humanize_time(amount, units):
        tokens.append('{} {}'.format(token[0], token[1]))
    return ', '.join(tokens)
