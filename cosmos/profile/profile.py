"""
example output:
{
    "avg_num_threads": 1,
    "cpu_time": 0,
    "avg_vms_mem_kb": 11427840,
    "io_read_kb": 4096,
    "io_write_kb": 0,
    "max_num_threads": 1,
    "system_time": 0,
    "max_rss_mem_kb": 1470464,
    "percent_cpu": 0,
    "max_vms_mem_kb": 11427840,
    "wall_time": 2,
    "ctx_switch_voluntary": 12,
    "user_time": 0,
    "avg_num_fds": 4,
    "num_polls": 1,
    "max_num_fds": 4,
    "io_write_count": 0,
    "avg_rss_mem_kb": 1470464,
    "ctx_switch_involuntary": 3,
    "io_read_count": 12,
    "exit_status": 0
}
"""
from __future__ import division
import time
import itertools as it
from collections import OrderedDict, defaultdict, namedtuple
import os
import signal
import json
import sys

import psutil


data_type = namedtuple('data_type', ['new_name', 'category'])
SCHEMA = OrderedDict([
    ('user', data_type('user_time', 'cpu_times')),
    ('system', data_type('system_time', 'cpu_times')),
    ('rss', data_type('rss_mem_kb', 'memory_info')),
    ('vms', data_type('vms_mem_kb', 'memory_info')),
    ('read_count', data_type('read_count', 'io_counters')),
    ('read_bytes', data_type('io_read_kb', 'io_counters')),
    ('write_bytes', data_type('io_write_kb', 'io_counters')),
    ('write_count', data_type('io_write_count', 'io_counters')),
    ('num_fds', data_type('num_fds', 'num_fds')),
    ('voluntary', data_type('ctx_switch_voluntary', 'num_ctx_switches')),
    ('involuntary', data_type('ctx_switch_involuntary', 'num_ctx_switches')),
    ('num_threads', data_type('num_threads', 'num_threads')),
])

def _mean(values):
    n = 0.0
    total = 0.0
    for v in values:
        if v is not None:
            total += v
            n += 1
    if n == 0:
        return 0
    else:
        return int(total / n)


def _max(values):
    try:
        return max(values)
    except ValueError:
        return 0


def _poll(p):
    """
    Polls a process
    :yields: (attribute_name, value)
    """

    def _human_readable(field, value):
        new_name = SCHEMA[field].new_name
        if 'kb' in new_name:
            value = int(value /1024.)
        return new_name, value


    attrs = ['cpu_times', 'memory_info', 'io_counters', 'num_fds', 'num_ctx_switches', 'num_threads']
    for category in attrs:
        try:
            r = getattr(p, 'get_' + category)()
        except (psutil.AccessDenied, psutil.NoSuchProcess):
            continue

        if hasattr(r, '_fields'):
            for field in r._fields:
                value = getattr(r, field)
        else:
            field, value = category, r

        yield _human_readable(field, value)


def _poll_children(p):
    """
    :yields: (attribute_name, the sum of all polled values of a process and it's children)
    """
    polls = (_poll(child) for child in it.chain(p.get_children(recursive=True), [p]))
    for tuples in it.izip(*polls):
        name = tuples[0][0]
        yield name, sum(value for _, value in tuples)


def main(command_script, poll_interval=1, output_file=None):
    command_script = os.path.abspath(command_script)
    try:
        # Declare data store variables
        records = defaultdict(list)
        output = OrderedDict()
        for place_holder in ['percent_cpu', 'wall_time', 'cpu_time', 'avg_rss_mem_kb', 'avg_vms_mem_kb', 'max_rss_mem_kb', 'max_vms_mem_kb']:
            output[place_holder] = None

        # Run the command and do the polling
        start_time = time.time()
        proc = psutil.Popen(command_script)
        num_polls = 0
        while proc.poll() is None:
            num_polls += 1
            for name, value in _poll_children(proc):
                if name in ['rss_mem_kb', 'vms_mem_kb', 'num_threads', 'num_fds']:
                    # TODO consolidate values to avoid using too much ram.  need to save max to do this
                    # if num_polls % 3600 == 0:
                    # records[name] = [_mean(records[name])]

                    records[name].append(value)
                else:
                    output[name] = int(value)

            time.sleep(poll_interval)

    except KeyboardInterrupt:
        print >> sys.stderr, 'Caught a SIGINT (ctrl+c), terminating'
        os.kill(proc.pid, signal.SIGINT)

    # Get means and maxes
    for name in ['rss_mem_kb', 'vms_mem_kb', 'num_threads', 'num_fds']:
        output['avg_%s' % name] = _mean(records[name])
        output['max_%s' % name] = _max(records[name])

    # Calculate some extra fielsd
    output['exit_status'] = proc.poll()
    end_time = time.time()  # waiting till last second
    output['num_polls'] = num_polls
    output['wall_time'] = int(end_time - start_time)
    if output.get('cpu_time'):
        output['percent_cpu'] = int(round(float(output.get('cpu_time', 0) / float(output['wall_time']), 2) * 100))
    else:
        output['percent_cpu'] = 0
    output['cpu_time'] = output.get('user_time', 0) + output.get('system_time', 0)


    # Write output
    output_json = json.dumps(output, indent=4)
    if output_file:
        with open(output_file, 'w') as fh:
            fh.write(output_json)
    else:
        print >> sys.stdout, output_json

    sys.exit(output['exit_status'])


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Profile resource usage of a command')
    parser.add_argument('-f', '--output_file', help='File to store output of profile to.')
    parser.add_argument('-i', '--poll_interval', type=int, default=2,
                        help='How often to poll the resource usage information in /proc, in seconds.')
    # parser.add_argument('command', nargs=argparse.REMAINDER, help="The command to run. Required.")
    parser.add_argument('command_script', help="path to a shell script to run")
    args = parser.parse_args()

    # make sure command script exists, sometimes on a shared filesystem it can take a while to propogate (i.e. eventual consistency)
    start = time.time()
    while not os.path.exists(args.command_script):
        time.sleep(.5)
        if time.time() - start > 20:
            raise IOError('giving up on %s existing' % args.command)

    if args.output_file is not None and os.path.exists(args.output_file):
        os.unlink(args.output_file)

    # Run Profile
    main(**vars(args))