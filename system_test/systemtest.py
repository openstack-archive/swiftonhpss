#!/usr/bin/env python
#
# Copyright (c) 2015-2016 IBM Corporation
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

import swiftclient
import multiprocessing
import os
import time
import ConfigParser
import collections
import itertools
from pprint import pprint as pretty_print


# This is here because you can't normally do multiprocessing on
# instance methods. We get around that restriction by using this
# top-level function in the worker process as the execution target
# to call it for us, with the class state and method name pickled and
# piped in through a queue. Who needs the Law of Demeter anyway?!
def _call_instance_method(instance, method_name, *args, **kwargs):
    return getattr(instance.__class__, method_name)(instance, *args, **kwargs)


def _parse_config():
    parser = ConfigParser.SafeConfigParser()
    parser.read('systemtest.cfg')
    options = {}
    for opt in ['authurl', 'username', 'tenantname',
                'password', 'num_workers', 'num_cycles',
                'hpss_storage_policy']:
        options[opt] = parser.get('config', opt)

    # These are specifically numbers.
    options['num_workers'] = int(options['num_workers'])
    options['num_cycles'] = int(options['num_cycles'])
    return options


def apply_ansi(string_to_format, ansi_codes):
    input_codes = ansi_codes
    if type(input_codes) is int:
        input_codes = [ansi_codes]
    if len(input_codes) is 0:
        return string_to_format
    code_str = ";".join(map(str, input_codes))
    return '\033[%sm%s\033[0m' % (code_str, string_to_format)


def average(value_list):
    if len(value_list) == 0:
        return 0
    return sum(value_list) / len(value_list)


def chain(value, *fns):
    result = value
    for fn in fns:
        result = fn(result)
    return result


def human_readable_size(size, over_time=True):
    if over_time:
        prefixes = ['B/s', 'KiB/s', 'MiB/s', 'GiB/s', 'TiB/s', 'PiB/s', 'EiB/s']
    else:
        prefixes = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB']
    tmp = size
    magnitude = 0
    while tmp >= 1024 and magnitude < len(prefixes)-1:
        tmp /= 1024.0
        magnitude += 1
    return '%f %s' % (tmp, prefixes[magnitude])


ansi_modes = {'clear': 0,
              'bold': 1,
              'blink': 5,
              'fg_black': 30,
              'fg_red': 31,
              'fg_green': 32,
              'fg_yellow': 33,
              'fg_blue': 34,
              'fg_magenta': 35,
              'fg_cyan': 36,
              'fg_white': 37,
              'bg_black': 40,
              'bg_red': 41,
              'bg_green': 42,
              'bg_yellow': 43,
              'bg_blue': 44,
              'bg_magenta': 45,
              'bg_cyan': 46,
              'bg_white': 47}

# These three things are all classes in their own right.
IORecord = collections.namedtuple('IORecord', ['size', 'elapsed_time'])

TestWorkerRecord = collections.namedtuple('TestWorkerRecord',
                                          ['handle', 'queue'])

TestResultRecord = collections.namedtuple('TestResultRecord',
                                          ['worker_pid', 'write_result',
                                           'read_result', 'test_size', 'cycle'])


class TimedReader:
    def __init__(self, source, size=None):
        self.source = source
        self.read_samples = []
        self.done = False
        self.last_read_time = time.time()
        self.size = size
        self._total_read = 0

    def read(self, size):
        current = time.time()
        result = self.source.read(size)
        bytes_read = len(result)
        self.read_samples.append(
            IORecord(size=bytes_read,
                     elapsed_time=current - self.last_read_time))
        self._total_read += bytes_read
        self.last_read_time = current
        if result == '' or (self._total_read >= self.size):
            self.done = True
        return result

    def reset(self):
        self.source.seek(0)
        self.read_samples = []
        self.done = False
        self.last_read_time = time.time()
        self._total_read = 0

    def time_so_far(self):
        return sum([rec.elapsed_time for rec in self.read_samples])

    def read_so_far(self):
        return sum([rec.size for rec in self.read_samples])

    def io_rate(self):
        return self.read_so_far() / self.time_so_far()


class PerformanceTestWorker():

    def __init__(self, args, queue):
        self.pid = None
        self.args = args
        self.queue = queue
        self.files_written = []
        self.swift_connection = None
        self.results = []

    # This process is what the controller process calls to start execution.
    def start(self, sem):
        worker = multiprocessing.Process(target=_call_instance_method,
                                         args=(self, '_do_it', sem))
        worker.start()

    # This function gets bootstrapped on the remote process side.
    def _do_it(self, semaphore):
        semaphore.acquire()
        try:
            self.pid = os.getpid()
            num_cycles = int(self.args['num_cycles'])
            MEGABYTE = 1024*1024
            sizes = [1*MEGABYTE, 4*MEGABYTE, 16*MEGABYTE, 64*MEGABYTE]
            self.swift_connection = swiftclient.client.Connection(
                authurl=self.args['authurl'],
                user=self.args['username'],
                key=self.args['password'],
                tenant_name=self.args['tenantname'],
                auth_version='2')
            for cycle in xrange(1, num_cycles+1):
                for size in sizes:
                    result = self._test_cycle(size, cycle)
                    self.results.append(result)
        except:
            self.queue.put([])
            raise
        else:
            self.queue.put(self.results)
        finally:
            semaphore.release()

    def _test_cycle(self, size, cycle):
        zero_name = '%d-%d-zero-%d' % (size, self.pid, cycle)
        random_name = '%d-%d-random-%d' % (size, self.pid, cycle)

        zero_file = open('/tmp/%s' % zero_name, 'w')
        random_file = open('/tmp/%s' % random_name, 'w')

        # Create files to read from, to make test more accurate
        zero_file.write(open('/dev/zero').read(size))
        random_file.write(open('/dev/urandom').read(size))

        zero_file.close()
        random_file.close()

        zero_file = open('/tmp/%s' % zero_name, 'r')
        random_file = open('/tmp/%s' % random_name, 'r')

        # Write the file
        zero_timer = TimedReader(zero_file, size)
        self.swift_connection.put_object('.performance_test',
                                         zero_name,
                                         zero_timer, size)
        random_timer = TimedReader(random_file, size)
        self.swift_connection.put_object('.performance_test',
                                         random_name,
                                         random_timer, size)
        write_result = {'zero_avg': zero_timer.io_rate(),
                        'random_avg': random_timer.io_rate(),
                        }

        # Read the file back
        zero_stream = \
            self.swift_connection.get_object('.performance_test', zero_name,
                                             resp_chunk_size=65536)[1]
        random_stream = \
            self.swift_connection.get_object('.performance_test', random_name,
                                             resp_chunk_size=65536)[1]
        zero_timer = TimedReader(zero_stream, size)
        random_timer = TimedReader(random_stream, size)
        read_size = 65536
        while not (zero_timer.done or random_timer.done):
            zero_timer.read(read_size)
            random_timer.read(read_size)
        read_result = {'zero_avg': zero_timer.io_rate(),
                       'random_avg': random_timer.io_rate(),
                       }

        # Cleanup
        self.swift_connection.delete_object('.performance_test', zero_name)
        self.swift_connection.delete_object('.performance_test', random_name)
        os.unlink('/tmp/%s' % zero_name)
        os.unlink('/tmp/%s' % random_name)

        return TestResultRecord(write_result=write_result,
                                read_result=read_result,
                                worker_pid=self.pid,
                                test_size=size,
                                cycle=cycle)


class PerformanceTestController():

    def __init__(self, args):
        self.args = args
        self.semaphore = None
        self.workers = []

    def create_worker(self):
        queue = multiprocessing.Queue()
        worker = PerformanceTestWorker(self.args, queue)
        self.workers.append(TestWorkerRecord(worker, queue))

    def start_test(self):
        self.semaphore = multiprocessing.Semaphore(len(self.workers))
        for worker_record in self.workers:
            worker_record.handle.start(self.semaphore)

    def is_done(self):
        if not self.semaphore:
            return False
        return self.semaphore.get_value() == len(self.workers)

    def get_results(self):
        worker_results = [worker.queue.get() for worker in self.workers]
        results = list(itertools.chain(*worker_results))

        # This next bit only works because Python's sort algorithm is stable.
        # What this is supposed to do is sort this list so that we have
        # the test size as the primary sort key, then worker_pid, then cycle.
        # The order matters.
        results = sorted(results, key=lambda x: x.cycle)
        results = sorted(results, key=lambda x: x.worker_pid)
        results = sorted(results, key=lambda x: x.test_size)
        return results


# TODO: is there any nicer way to handle the view for this test?
# maybe in the future some fancy graphical view? something to copy-paste
# into slide decks
class PerformanceTestViewer():

    def __init__(self, results):
        self.results = results

    def title(self, content):
        return apply_ansi(content, ansi_modes['bold'])

    def header(self, content):
        return apply_ansi(content, ansi_modes['fg_green'])

    def error(self, content):
        return apply_ansi(content, ansi_modes['fg_red'])

    def _group_by(self, iterable, keyfunc):
        return {k: list(v) for k, v
                in itertools.groupby(iterable, key=keyfunc)}

    # TODO: make this much more concise
    def show_results(self):
        size_to_results = self._group_by(self.results,
                                         keyfunc=lambda x: x.test_size)

        pretty_print(size_to_results)
        for size in size_to_results:
            pid_to_results = self._group_by(size_to_results[size],
                                            keyfunc=lambda x: x.worker_pid)
            print self.title('Testing with files of size %s:' %
                             human_readable_size(size, over_time=False))
            print '---'

            print self.header('  Ingesting random bytes:')
            for pid in pid_to_results:
                print self.header('    Worker %s:' % pid), \
                    map(lambda res: human_readable_size(
                            res.write_result['random_avg']),
                        pid_to_results[pid])
            print self.header('    Overall average per worker:'), \
                chain(map(lambda res: res.write_result['random_avg'],
                          size_to_results[size]), average, human_readable_size)
            print ''

            print self.header('  Ingesting zero bytes:')
            for pid in pid_to_results:
                print self.header('    Worker %s:' % pid), \
                    map(lambda res: human_readable_size(
                            res.write_result['zero_avg']),
                        pid_to_results[pid])
            print self.header('    Overall average per worker:'), \
                chain(map(lambda res: res.write_result['zero_avg'],
                          size_to_results[size]), average, human_readable_size)
            print ''

            print self.header('  Recalling random bytes:')
            for pid in pid_to_results:
                print self.header('    Worker %s:' % pid), \
                    map(lambda res: human_readable_size(
                            res.read_result['random_avg']),
                        pid_to_results[pid])
            print self.header('    Overall average per worker:'), \
                chain(map(lambda res: res.read_result['random_avg'],
                          size_to_results[size]), average, human_readable_size)
            print ''

            print self.header('  Recalling zero bytes:')
            for pid in pid_to_results:
                print self.header('    Worker %s:' % pid), \
                    map(lambda res: human_readable_size(
                        res.read_result['zero_avg']),
                        pid_to_results[pid])
            print self.header('    Overall average per worker:'), \
                chain(map(lambda res: res.read_result['zero_avg'],
                          size_to_results[size]), average, human_readable_size)
            print '---'
            print ''


def main():
    args = _parse_config()
    worker_count = int(args['num_workers'])
    swift = swiftclient.client.Connection(authurl=args['authurl'],
                                          user=args['username'],
                                          tenant_name=args['tenantname'],
                                          key=args['password'],
                                          auth_version='2')
    swift.put_container('.performance_test',
                        {'X-Storage-Policy':
                         args['hpss_storage_policy']})
    controller = PerformanceTestController(args)
    for i in xrange(0, worker_count):
        controller.create_worker()
    print "Starting %d worker(s)..." % worker_count
    controller.start_test()
    print "Waiting for test results..."
    # Wait for all "threads" to finish
    time.sleep(.5)
    while not controller.is_done():
        time.sleep(.1)
    all_results = controller.get_results()
    view = PerformanceTestViewer(all_results)
    view.show_results()

if __name__ == "__main__":
    main()
