import time
from kosmos.profile import read_man_proc

start_time = time.time()

import subprocess, sys,re,os,sqlite3,json,signal
import logging
import argparse

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


class Profile:
    fields_to_get = {'UPDATE': ['VmPeak','VmHWM'] + #/proc/status
                               ['minflt','majflt','utime','stime','delayacct_blkio_ticks','voluntary_ctxt_switches','nonvoluntary_ctxt_switches'], #/proc/stat removed 
                     'INSERT': ['FDSize','VmSize','VmLck','VmRSS','VmData','VmLib','VmPTE'] + #/proc/status
                               ['num_threads'] #/proc/stat
                     }
    
    proc = None #the parse_args subprocess object
    poll_number = 0 #number of polls so far
    
    @property
    def all_pids(self):
        """This parse_args process and all of its descendant's pids"""
        return self.and_descendants(os.getpid())
    
    def __init__(self,command,poll_interval=1,output_file=None,database_file=':memory:'):
        def add_quotes(arg):
            "quotes get stripped off by the shell when it interprets the command, so this adds them back in"
            if re.search("\s",arg):
                return "\""+arg+"\""
            else: return arg
        self.command = ' '.join(map(add_quotes, command))
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
        self.c.execute("CREATE TABLE process (pid INTEGER PRIMARY KEY, poll_number INTEGER, name TEXT, {0})".format(sqfields))
        
        #setup logging
        self.log=logging

    def _unnest(self,a_list):
        """
        unnests a list
        
        .. example::
        >>> _unnest([1,2,[3,4],[5]])
        [1,2,3,4,5]
        """
        return [ item for items in a_list for item in items ]

    def get_children(self,pid):
        "returns a list of this pid's children"
        p = subprocess.Popen('/bin/ps h --ppid {0} -o pid'.format(pid).split(' '),shell=False,stdout=subprocess.PIPE)
        children = map(lambda x: x.strip(),filter(lambda x: x!='',p.communicate()[0].strip().split('\n')))
        return children
    
    def and_descendants(self,pid):
        "Returns a list of this pid and all of its descendant process (children's children, etc) ids"
        children = self.get_children(pid)
        
        if len(children) == 0:
            return [pid]
        else: 
            return [pid] + self._unnest([ self.and_descendants(int(child)) for child in children ])
    
    def run(self):
        """
        Runs a process and records the memory usage of it and all of its descendants"""
        self.proc = subprocess.Popen(self.command,shell=True)
        while True:
            self.poll_all_procs(pids=self.all_pids)
            
            time.sleep(self.poll_interval)
            if self.proc.poll() != None:
                self.finish()
    
    def parseVal(self,val):
        "Remove kB and return ints."
        return int(val) if val[-2:] != 'kB' else int(val[0:-3])
        
    def poll_all_procs(self,pids):
        """Updates the sql table with all descendant processes' resource usage"""
        self.poll_number = self.poll_number + 1
        for pid in pids:
            try:
                all_stats = self.read_proc_stat(pid)
                all_stats += self.read_proc_status(pid)
                #Inserts
                inserts = [ (name,self.parseVal(val)) for name,val in all_stats if name in self.fields_to_get['INSERT'] ] + [('pid',pid),('poll_number',self.poll_number)]
                keys,vals = zip(*inserts) #unzip
                q = "INSERT INTO record ({keys}) values({s})".format(s = ', '.join(['?']*len(vals)),
                                                                     keys = ', '.join(keys))
                self.c.execute(q,vals)
                #Updates
                proc_name = filter(lambda x: x[0]=='Name' ,all_stats)[0][1]
                updates = [ (name,self.parseVal(val)) for name,val in all_stats if name in self.fields_to_get['UPDATE'] ] + [('pid',pid),('Name',proc_name),('poll_number',self.poll_number)]
                keys,vals = zip(*updates) #unzip
                q = "INSERT OR REPLACE INTO process ({keys}) values({s})".format(s = ', '.join(['?']*len(vals)),
                                                                     keys = ', '.join(keys))
                self.c.execute(q,vals)
                
            except IOError:
                pass # process finished before file could be read
                 
        
    def read_proc_stat(self,pid):
        """
        :returns: (field_name,value) from /proc/pid/stat or None if its empty
        """
        stat_fields = read_man_proc.get_stat_and_status_fields()
        with open('/proc/{0}/stat'.format(pid),'r') as f:
            stat_all = f.readline().split(' ')
            return map(lambda x: (x[0][0],x[1]),zip(stat_fields,stat_all))
        
    def read_proc_status(self,pid):
        """
        :returns: (field_name,value) from /proc/pid/status or None if its empty
        """
        def line2tuple(l):
            m = re.search(r"\s*(.+):\s*(.+)\s*",l)
            return m.group(1),m.group(2)
        with open('/proc/{0}/status'.format(pid),'r') as f:
            return map(line2tuple,f.readlines())
        
        
    def analyze_records(self):
        """
        Summarizes and aggregates all the resource usage of self.all_pids
        :returns: a dictionary of profiled statistics
        """
        #aggregate and summarize all the polls
        self.c.execute("""
            --average and max of cross sections at each poll
            SELECT
                num_polls,
                num_processes,
                MAX(FDSize) as max_fdsize, AVG(FDSize) as avg_fdsize, 
                MAX(VmSize) as max_virtual_mem, AVG(VmSize) as avg_virtual_mem, 
                MAX(VmLck) as max_locked_mem, AVG(VmLck) as avg_locked_mem, 
                MAX(VmRSS) as max_rss_mem, AVG(VmRSS) as avg_rss_mem, 
                MAX(VmData) as max_data_mem, AVG(VmData) as avg_data_mem, 
                MAX(VmLib) as max_lib_mem, AVG(VmLib) as avg_lib_mem, 
                MAX(VmPTE) as max_pte_mem, AVG(VmPTE) as avg_pte_mem,
                MAX(num_threads) as max_num_threads, AVG(num_threads) as avg_num_threads
            FROM
            --sum up resource usage of all processes at each poll (=1 cross section)
            (SELECT
                Max(poll_number) as num_polls,
                Count(pid) as num_processes,
                SUM(FDSize) as FDSize, 
                SUM(VmSize) as VmSize,
                SUM(VmRSS) as VmRSS, 
                SUM(VmLck) as VmLck, 
                SUM(VmData) as VmData, 
                SUM(VmLib) as VmLib,  
                SUM(VmPTE) as VmPTE, 
                SUM(num_threads) as num_threads,
                SUM(FDSize) as FDSize
            FROM record
            GROUP BY poll_number )
            """)
        keys = [ x[0] for x in self.c.description]
        profiled_inserts = zip(keys,self.c.next())
        
        #Summarize the updates
        self.c.execute("""
            SELECT
                group_concat(name) as names,
                group_concat(pid) as pids,
                SUM(delayacct_blkio_ticks) as block_io_delays,
                SUM(utime) as user_time,
                SUM(stime) as system_time,
                SUM(majflt) as major_page_faults,
                SUM(minflt) as minor_page_faults,
                MAX(VmPeak) as single_proc_max_peak_virtual_mem,
                MAX(VmHWM) as single_proc_max_peak_rss,
                SUM(voluntary_ctxt_switches) as voluntary_context_switches,
                SUM(nonvoluntary_ctxt_switches) as nonvoluntary_context_switches
            FROM process
            """)
        
        keys = [ x[0] for x in self.c.description]
        profiled_updates = zip(keys,self.c.next())
        
#        self.c.execute("SELECT * FROM process")
#        keys = [ x[0] for x in self.c.description]
#        self.log.debug([dict(zip(keys,vals)) for vals in self.c])
        
        profiled_procs = dict(profiled_inserts + profiled_updates)
        SC_CLK_TCK = os.sysconf(os.sysconf_names['SC_CLK_TCK'])
        
        for time_var in ['user_time','system_time','block_io_delays']: #convert to seconds
            profiled_procs[time_var] = int(profiled_procs[time_var] / SC_CLK_TCK)
        
        profiled_procs['cpu_time'] = profiled_procs['user_time'] + profiled_procs['system_time']
        profiled_procs['exit_status'] = self.proc.poll()
        #profiled_procs['SC_CLK_TCK'] = SC_CLK_TCK #usually is 100, or centiseconds 
        
        end_time = time.time() #waiting till last second
        profiled_procs['wall_time'] = round(end_time - start_time)
        profiled_procs['percent_cpu'] = int(round(float(profiled_procs['cpu_time'])/float(profiled_procs['wall_time']),2)*100)
        return profiled_procs
    
    def finish(self):
        """Executed when self.proc has finished"""
        result = self.analyze_records()
        if self.output_file != None:
            self.output_file.write(json.dumps(result,indent=4,sort_keys=True))
        else:
            print >>sys.stderr, json.dumps(result,indent=4,sort_keys=True)
        sys.exit(result['exit_status'])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('-f', '--file', type=argparse.FileType('w'), help='File to store output of profile to.')
    parser.add_argument('-i', '--interval', type=int, default=1, help='How often to poll the resource usage information in /proc, in seconds.')
    parser.add_argument('-db', '--dbfile', type=str, default=':memory:', help='File to store sqlite data to (default is in memory).  Will overwrite if the database already exists.')
    parser.add_argument('command', nargs=argparse.REMAINDER,help="The command to run. Required.")
    args = parser.parse_args()
    if len(args.command)==0:
        parser.print_help()
        sys.exit(1)

    #Run Profile
    profile = Profile(command=args.command,output_file=args.file,database_file=args.dbfile,poll_interval=args.interval)
    try:
        result = profile.run()
    except KeyboardInterrupt:
        os.kill(profile.proc.pid,signal.SIGINT)
        profile.finish()
    except:
        raise