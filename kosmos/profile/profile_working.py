"""
    Samples resource usage statistics about a process and all of its descendants from /proc/[pid]/status
    The output is a JSON dictionary that summarizes the resource usage

    polls these fields from /proc/pid/stat:
    see "man proc" for more info, or see read_man_proc.py
    minflt, cminflt, majflt, utime, stime, cutime, cstime, priority, nice, num_threads, exit_signal, delayacct_blkio_ticks
    *removed rsslim for now*

    polls these fields from /proc/pid/status:
    * FDSize: Number of file descriptor slots currently allocated.
    * VmPeak: Peak virtual memory size.
    * VmSize: Virtual memory size.
    * VmLck: Locked memory size (see mlock(3)).
    * VmHWM: Peak resident set size ("high water mark").
    * VmRSS: Resident set size.
    * VmData, VmStk, VmExe: Size of data, stack, and text segments.
    * VmLib: Shared library code size.
    * VmPTE: Page table entries size (since Linux 2.6.10).

    And returns:
    MAX(FDSize), AVG(FDSize), MAX(VmPeak), AVG(VmSize), MAX(VmLck), AVG(VmLck), AVG(VmRSS), AVG(VmData), MAX(VmData), AVG(VmLib), MAX(VmPTE), AVG(VmPTE)
"""

import psutil
import time

from kosmos.profile.read_man_proc import get_stat_and_status_fields


start_time = time.time()

import subprocess, sys, re, os, sqlite3, json, signal
import logging
import argparse


class Profile:
    fields_to_get = {'UPDATE': ['VmPeak', 'VmHWM'] +  # /proc/status
                               ['minflt', 'majflt', 'utime', 'stime', 'delayacct_blkio_ticks',
                                'voluntary_ctxt_switches', 'nonvoluntary_ctxt_switches'],  # /proc/stat removed
                     'INSERT': ['FDSize', 'VmSize', 'VmLck', 'VmRSS', 'VmData', 'VmLib', 'VmPTE'] +  #/proc/status
                               ['num_threads']}  # /proc/stat

    proc = None  # the subprocess instance
    poll_number = 0  # number of polls so far

    @property
    def all_pids(self):
        """This parse_args process and all of its descendant's pids"""
        pid = os.getpid()
        return [pid] + map(lambda p: p.pid, psutil.Process(pid).get_children(recursive=True))

    def __init__(self, command, poll_interval=1, output_file=None, database_file=':memory:'):
        # def add_quotes(arg):
        # "quotes get stripped off by the shell when it interprets the command, so this adds them back in"
        #     if re.search("\s", arg):
        #         return "\"" + arg + "\""
        #     else:
        #         return arg

        #self.command = ' '.join(map(add_quotes, command))
        self.command = command
        self.poll_interval = poll_interval
        self.output_file = output_file
        self.database_file = database_file

        #Setup SQLite
        if os.path.exists(database_file):
            os.unlink(database_file)
        self.conn = sqlite3.connect(database_file)
        self.c = self.conn.cursor()

        #Create Records Table
        insert_fields = self.fields_to_get['INSERT']
        sqfields = ', '.join(map(lambda x: x + ' INTEGER', insert_fields))
        self.c.execute("CREATE TABLE record (pid INTEGER, poll_number INTEGER, {0})".format(sqfields))
        #Create Processes Table
        update_fields = self.fields_to_get['UPDATE']
        sqfields = ', '.join(map(lambda x: x + ' INTEGER', update_fields))
        self.c.execute(
            "CREATE TABLE process (pid INTEGER PRIMARY KEY, poll_number INTEGER, name TEXT, {0})".format(sqfields))

        #setup logging
        self.log = logging

    def run(self):
        """
        Runs a process and records the resource usage of it and all of its descendants
        """
        self.log.info('exec: %s' % self.command)
        self.proc = subprocess.Popen(self.command)
        while True:
            self.poll_all_procs(pids=self.all_pids)

            time.sleep(self.poll_interval)
            if self.proc.poll() is not None:
                self.finish()

    @staticmethod
    def parse_val(val):
        """Remove kB and return ints."""
        return int(val) if val[-2:] != 'kB' else int(val[0:-3])

    def poll_all_procs(self, pids):
        """Updates the sql table with all descendant processes' resource usage"""
        self.poll_number += 1
        for pid in pids:
            try:
                all_stats = self.read_proc_stat(pid)
                all_stats += self.read_proc_status(pid)
                # Inserts
                inserts = [(name, self.parse_val(val)) for name, val in all_stats if
                           name in self.fields_to_get['INSERT']] + [('pid', pid), ('poll_number', self.poll_number)]
                keys, vals = zip(*inserts)  #unzip
                q = "INSERT INTO record ({keys}) values({s})".format(s=', '.join(['?'] * len(vals)),
                                                                     keys=', '.join(keys))
                self.c.execute(q, vals)
                #Updates
                proc_name = filter(lambda x: x[0] == 'Name', all_stats)[0][1]
                updates = [(name, self.parse_val(val)) for name, val in all_stats if
                           name in self.fields_to_get['UPDATE']] + [('pid', pid), ('Name', proc_name),
                                                                    ('poll_number', self.poll_number)]
                keys, vals = zip(*updates)  #unzip
                q = "INSERT OR REPLACE INTO process ({keys}) values({s})".format(s=', '.join(['?'] * len(vals)),
                                                                                 keys=', '.join(keys))
                self.c.execute(q, vals)

            except IOError:
                pass  # process finished before file could be read

    @staticmethod
    def read_proc_stat(pid):
        """
        :returns: (field_name,value) from /proc/pid/stat or None if its empty
        """
        stat_fields = get_stat_and_status_fields()
        with open('/proc/{0}/stat'.format(pid), 'r') as f:
            stat_all = f.readline().split(' ')
            return map(lambda x: (x[0][0], x[1]), zip(stat_fields, stat_all))

    @staticmethod
    def read_proc_status(pid):
        """
        :returns: (field_name,value) from /proc/pid/status or None if its empty
        """
        reg = re.compile(r"\s*(.+):\s*(.+)\s*")

        def line2tuple(l):
            m = re.search(reg, l)
            return m.group(1), m.group(2)

        with open('/proc/{0}/status'.format(pid), 'r') as f:
            return map(line2tuple, f.readlines())

    def analyze_records(self):
        """
        Summarizes and aggregates all the resource usage of self.all_pids
        :returns: a dictionary of profiled statistics
        """
        # aggregate and summarize all the polls
        self.c.execute("""
            --average and max of cross sections at each poll
            SELECT
                num_polls,
                num_processes,
                MAX(FDSize) AS max_fdsize, AVG(FDSize) AS avg_fdsize,
                MAX(VmSize) AS max_virtual_mem, AVG(VmSize) AS avg_virtual_mem,
                MAX(VmLck) AS max_locked_mem, AVG(VmLck) AS avg_locked_mem,
                MAX(VmRSS) AS max_rss_mem, AVG(VmRSS) AS avg_rss_mem,
                MAX(VmData) AS max_data_mem, AVG(VmData) AS avg_data_mem,
                MAX(VmLib) AS max_lib_mem, AVG(VmLib) AS avg_lib_mem,
                MAX(VmPTE) AS max_pte_mem, AVG(VmPTE) AS avg_pte_mem,
                MAX(num_threads) AS max_num_threads, AVG(num_threads) AS avg_num_threads
            FROM
            --sum up resource usage of all processes at each poll (=1 cross section)
            (SELECT
                Max(poll_number) AS num_polls,
                Count(pid) AS num_processes,
                SUM(FDSize) AS FDSize,
                SUM(VmSize) AS VmSize,
                SUM(VmRSS) AS VmRSS,
                SUM(VmLck) AS VmLck,
                SUM(VmData) AS VmData,
                SUM(VmLib) AS VmLib,
                SUM(VmPTE) AS VmPTE,
                SUM(num_threads) AS num_threads,
                SUM(FDSize) AS FDSize
            FROM record
            GROUP BY poll_number )
            """)
        keys = [x[0] for x in self.c.description]
        profiled_inserts = zip(keys, self.c.next())

        #Summarize the updates
        self.c.execute("""
            SELECT
                group_concat(name, ", ") AS names,
                group_concat(pid, ", ") AS pids,
                SUM(delayacct_blkio_ticks) AS block_io_delays,
                SUM(utime) AS user_time,
                SUM(stime) AS system_time,
                SUM(majflt) AS major_page_faults,
                SUM(minflt) AS minor_page_faults,
                MAX(VmPeak) AS single_proc_max_peak_virtual_mem,
                MAX(VmHWM) AS single_proc_max_peak_rss,
                SUM(voluntary_ctxt_switches) AS voluntary_context_switches,
                SUM(nonvoluntary_ctxt_switches) AS nonvoluntary_context_switches
            FROM process
            """)

        keys = [x[0] for x in self.c.description]
        profiled_updates = zip(keys, self.c.next())

        #        self.c.execute("SELECT * FROM process")
        #        keys = [ x[0] for x in self.c.description]
        #        self.log.debug([dict(zip(keys,vals)) for vals in self.c])

        d = dict(profiled_inserts + profiled_updates)
        sl_clk_tck = os.sysconf(os.sysconf_names['SC_CLK_TCK'])

        for time_var in ['user_time', 'system_time', 'block_io_delays']:  # convert to seconds
            d[time_var] = int(d[time_var] / sl_clk_tck)

        d['cpu_time'] = d['user_time'] + d['system_time']
        d['exit_status'] = self.proc.poll()
        #profiled_procs['SC_CLK_TCK'] = SC_CLK_TCK #usually is 100, or centiseconds 

        end_time = time.time()  # waiting till last second
        d['wall_time'] = round(end_time - start_time)
        d['percent_cpu'] = int(
            round(float(d['cpu_time']) / float(d['wall_time']), 2) * 100)
        return d

    def finish(self):
        """Executed when self.proc has finished"""
        result = self.analyze_records()
        if self.output_file:
            with open(self.output_file, 'w') as fh:
                fh.write(json.dumps(result, indent=4, sort_keys=True))
        else:
            print >> sys.stderr, json.dumps(result, indent=4, sort_keys=True)
        sys.exit(result['exit_status'])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('-f', '--output_file', help='File to store output of profile to.')
    parser.add_argument('-i', '--poll_interval', type=int, default=2,
                        help='How often to poll the resource usage information in /proc, in seconds.')
    parser.add_argument('-db', '--database_file', type=str, default=':memory:',
                        help='File to store sqlite data to (default is in memory).  '
                             'Will overwrite if the database already exists.')
    # parser.add_argument('command', nargs=argparse.REMAINDER, help="The command to run. Required.")
    parser.add_argument('command', help="path to a shell script to run")
    args = parser.parse_args()

    # makre sure command script exists, sometimes on a shared filesystem it can take a while to propogate (i.e. eventual consistency)
    start = time.time()
    while not os.path.exists(args.command):
        time.sleep(.5)
        if time.time() - start > 20:
            raise IOError('giving up on %s existing' % args.command)

    #Run Profile
    profile = Profile(**vars(args))
    try:
        profile.run()
    except KeyboardInterrupt:
        os.kill(profile.proc.pid, signal.SIGINT)
        profile.finish()