"""
Returns a sorted dictionary of all fields in /proc/pid/stat
"""
manpage ="""      pid %d      The process ID.

              comm %s     The filename of the executable, in parentheses.  This is
                          visible whether or not the executable is swapped out.

              state %c    One character from the string "RSDZTW" where R is running,
                          S is sleeping in an interruptible wait, D is waiting in
                          uninterruptible disk sleep, Z is zombie, T is traced or
                          stopped (on a signal), and W is paging.

              ppid %d     The PID of the parent.

              pgrp %d     The process group ID of the process.

              session %d  The session ID of the process.

              tty_nr %d   The controlling terminal of the process.  (The minor device
                          number is contained in the combination of bits 31 to 20 and
                          7 to 0; the major device number is in bits 15 to 8.)

              tpgid %d    The ID of the foreground process group of the controlling
                          terminal of the process.

              flags %u (%lu before Linux 2.6.22)
                          The kernel flags word of the process.  For bit meanings,
                          see the PF_* defines in <linux/sched.h>.  Details depend on
                          the kernel version.

              minflt %lu  The number of minor faults the process has made which have
                          not required loading a memory page from disk.
 
              cminflt %lu The number of minor faults that the process's waited-for
                          children have made.

              majflt %lu  The number of major faults the process has made which have
                          required loading a memory page from disk.

              cmajflt %lu The number of major faults that the process's waited-for
                          children have made.

              utime %lu   Amount of time that this process has been scheduled in user
                          mode, measured in clock ticks (divide by
                          sysconf(_SC_CLK_TCK).  This includes guest time, guest_time
                          (time spent running a virtual CPU, see below), so that
                          applications that are not aware of the guest time field do
                          not lose that time from their calculations.

              stime %lu   Amount of time that this process has been scheduled in
                          kernel mode, measured in clock ticks (divide by
                          sysconf(_SC_CLK_TCK).

              cutime %ld  Amount of time that this process's waited-for children have
                          been scheduled in user mode, measured in clock ticks
                          (divide by sysconf(_SC_CLK_TCK).  (See also times(2).)
                          This includes guest time, cguest_time (time spent running a
                          virtual CPU, see below).

              cstime %ld  Amount of time that this process's waited-for children have
                          been scheduled in kernel mode, measured in clock ticks
                          (divide by sysconf(_SC_CLK_TCK).

              priority %ld
                          (Explanation for Linux 2.6) For processes running a real-
                          time scheduling policy (policy below; see
                          sched_setscheduler(2)), this is the negated scheduling
                          priority, minus one; that is, a number in the range -2 to
                          -100, corresponding to real-time priorities 1 to 99.  For
                          processes running under a non-real-time scheduling policy,
                          this is the raw nice value (setpriority(2)) as represented
                          in the kernel.  The kernel stores nice values as numbers in
                          the range 0 (high) to 39 (low), corresponding to the user-
                          visible nice range of -20 to 19.

                          Before Linux 2.6, this was a scaled value based on the
                          scheduler weighting given to this process.

              nice %ld    The nice value (see setpriority(2)), a value in the range
                          19 (low priority) to -20 (high priority).

              num_threads %ld
                          Number of threads in this process (since Linux 2.6).
                          Before kernel 2.6, this field was hard coded to 0 as a
                          placeholder for an earlier removed field.

              itrealvalue %ld
                          The time in jiffies before the next SIGALRM is sent to the
                          process due to an interval timer.  Since kernel 2.6.17,
                          this field is no longer maintained, and is hard coded as 0.

              starttime %llu (was %lu before Linux 2.6)
                          The time in jiffies the process started after system boot.

              vsize %lu   Virtual memory size in bytes.

              rss %ld     Resident Set Size: number of pages the process has in real
                          memory.  This is just the pages which count toward text,
                          data, or stack space.  This does not include pages which
                          have not been demand-loaded in, or which are swapped out.

              rsslim %lu  Current soft limit in bytes on the rss of the process; see
                          the description of RLIMIT_RSS in getpriority(2).

              startcode %lu
                          The address above which program text can run.

              endcode %lu The address below which program text can run.

              startstack %lu
                          The address of the start (i.e., bottom) of the stack.

              kstkesp %lu The current value of ESP (stack pointer), as found in the
                          kernel stack page for the process.

              kstkeip %lu The current EIP (instruction pointer).

              signal %lu  The bitmap of pending signals, displayed as a decimal
                          number.  Obsolete, because it does not provide information
                          on real-time signals; use /proc/[pid]/status instead.

              blocked %lu The bitmap of blocked signals, displayed as a decimal
                          number.  Obsolete, because it does not provide information
                          on real-time signals; use /proc/[pid]/status instead.

              sigignore %lu
                          The bitmap of ignored signals, displayed as a decimal
                          number.  Obsolete, because it does not provide information
                          on real-time signals; use /proc/[pid]/status instead.

              sigcatch %lu
                          The bitmap of caught signals, displayed as a decimal
                          number.  Obsolete, because it does not provide information
                          on real-time signals; use /proc/[pid]/status instead.

              wchan %lu   This is the "channel" in which the process is waiting.  It
                          is the address of a system call, and can be looked up in a
                          namelist if you need a textual name.  (If you have an up-
                          to-date /etc/psdatabase, then try ps -l to see the WCHAN
                          field in action.)

              nswap %lu   Number of pages swapped (not maintained).

              cnswap %lu  Cumulative nswap for child processes (not maintained).

              exit_signal %d (since Linux 2.1.22)
                          Signal to be sent to parent when we die.

              processor %d (since Linux 2.2.8)
                          CPU number last executed on.

              rt_priority %u (since Linux 2.5.19; was %lu before Linux 2.6.22)
                          Real-time scheduling priority, a number in the range 1 to
                          99 for processes scheduled under a real-time policy, or 0,
                          for non-real-time processes (see sched_setscheduler(2)).

              policy %u (since Linux 2.5.19; was %lu before Linux 2.6.22)
                          Scheduling policy (see sched_setscheduler(2)).  Decode
                          using the SCHED_* constants in linux/sched.h.

              delayacct_blkio_ticks %llu (since Linux 2.6.18)
                          Aggregated block I/O delays, measured in clock ticks
                          (centiseconds).

              guest_time %lu (since Linux 2.6.24)
                          Guest time of the process (time spent running a virtual CPU
                          for a guest operating system), measured in clock ticks
                          (divide by sysconf(_SC_CLK_TCK).

              cguest_time %ld (since Linux 2.6.24)
                          Guest time of the process's children, measured in clock
                          ticks (divide by sysconf(_SC_CLK_TCK)."""

import re

def yield_fields(manpage):
    for i,g in enumerate(re.findall("(\w+)\s+(%\w+).*",manpage.strip())):
        yield g[0],i
        
def get_stat_and_status_fields():
    """
    Returns a list of all the items in /proc/pid/stat
    [('name of field1', column #1),
    ('name of field2', column #2),
    ('name of field3', column #3)]
    
    You can easily convert to a dict using dict(get_stat_and_status_fields())
    """
    return [ field for field in yield_fields(manpage) ]
if __name__ == '__main__':
    import pprint
    pprint.pprint(get_stat_and_status_fields(),indent=2)