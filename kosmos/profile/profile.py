"""
example output:
{
    "avg_num_fds": 0,
    "avg_num_threads": 1,
    "avg_rss_mem": 0,
    "avg_vms_mem": 0,
    "cpu_time": 0,
    "ctx_switch_involuntary": 2,
    "ctx_switch_voluntary": 11,
    "exit_status": 0,
    "max_num_fds": 0,
    "max_num_threads": 1,
    "max_rss_mem": 0,
    "max_vms_mem": 0,
    "num_polls": 1,
    "percent_cpu": 0,
    "system_time": 0,
    "user_time": 0,
    "wall_time": 2
}
"""
import time
import itertools as it
import psutil
from collections import OrderedDict, defaultdict
import os
import signal
import json
import sys

CATEGORIES = dict(rss='memory_info',
                  vms='memory_info',
                  num_fds='num_fds',
                  voluntary='num_ctx_switches',
                  involuntary='num_ctx_switches',
                  num_threads='num_threads',
                  read_count='io_counters',
                  write_count='io_counters',
                  read_bytes='io_counters',
                  write_bytes='io_counters',
                  user='cpu_times',
                  system='cpu_times')


def name2category(name):
    return CATEGORIES.get(name.replace('avg_', '').replace('max_', ''), None)


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
    attrs = ['cpu_times', 'memory_info', 'io_counters', 'num_fds', 'num_ctx_switches', 'num_threads']
    for a in attrs:
        try:
            r = getattr(p, 'get_' + a)()
        except (psutil.AccessDenied, psutil.NoSuchProcess):
            continue

        if hasattr(r, '_fields'):
            for field in r._fields:
                yield field, getattr(r, field)
        else:
            yield a, r


def _poll_children(p):
    polls = (_poll(child) for child in it.chain(p.get_children(recursive=True), [p]))
    for tuples in it.izip(*polls):
        yield tuples[0][0], sum(value for _, value in tuples)


def main(command_script, poll_interval=1, output_file=None):
    try:
        # Declare data stores
        records = defaultdict(list)
        output = OrderedDict()
        for place_holder in ['percent_cpu', 'wall_time', 'cpu_time']:
            output[place_holder] = None

        # Run the command and do the polling
        start_time = time.time()
        proc = psutil.Popen(command_script)
        num_polls = 0
        while proc.poll() is None:
            num_polls += 1
            for name, value in _poll_children(proc):
                if name in ['rss', 'vms', 'num_threads', 'num_fds']:
                    records[name].append(value)
                else:
                    output[name] = int(value)
            time.sleep(poll_interval)

    except KeyboardInterrupt:
        print >> sys.stderr, 'Caught a SIGINT (ctrl+c), terminating'
        os.kill(proc.pid, signal.SIGINT)

    for name in ['rss', 'vms', 'num_threads', 'num_fds']:
        output['avg_%s' % name] = _mean(records[name])
        output['max_%s' % name] = _max(records[name])

    output['exit_status'] = proc.poll()
    end_time = time.time()  # waiting till last second
    output['num_polls'] = num_polls
    output['wall_time'] = int(end_time - start_time)
    if output.get('walltime') and output.get('cpu_time'):
        output['percent_cpu'] = int(round(float(output['cpu_time']) / float(output['wall_time']), 2) * 100)
    else:
        output['percent_cpu'] = 0
    output['cpu_time'] = output['user'] + output['system']


    # # #
    # Write output
    # # #

    def human_readable(data):
        for name, value in data.items():
            c = name2category(name)
            if c == 'cpu_times':
                name += '_time'
            elif c == 'io_counters':
                name = 'io_' + name
            elif c == 'num_ctx_switches':
                name = 'ctx_switch_' + name
            elif c == 'memory_info':
                name += '_mem'

            yield name, value

    output_json = json.dumps(dict(human_readable(output)), indent=4)
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