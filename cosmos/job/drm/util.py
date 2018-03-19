import os
import subprocess


class DetailedCalledProcessError(subprocess.CalledProcessError):
    """
    Just like CalledProcessError, but includes stderr.
    """

    def __init__(self, returncode, cmd, output=None, stderr=None):
        super(DetailedCalledProcessError, self).__init__(returncode, cmd, output)
        self.stderr = stderr

    def __str__(self):
        err_str = '\nCMD_ERR: %s' % (self.stderr if self.stderr is not None else '')
        return "Command '%s' returned non-zero exit status %d.\nCMD_OUT: %s%s" % (self.cmd,
                                                                                     self.returncode,
                                                                                     self.output,
                                                                                     err_str)


def check_output_detailed_error(*args, **kwargs):
    try:
        return subprocess.check_output(*args, **kwargs)
    except subprocess.CalledProcessError as e:
        raise DetailedCalledProcessError(e.returncode, e.cmd, e.output)


def check_output_and_stderr(*popenargs, **kwargs):
    """
    Run command with arguments and return its stdout and stderr as byte strings.

    Lifted from the subprocess.check_output() implementation, to which it is
    identical, save that it returns a (stdout, stderr) tuple as opposed to
    simply stdout.

    Note that the CalledProcessError object raised by this method does not
    contain any stderr -- same (confusing) behavior as subprocess.check_output().
    """
    if 'stdout' in kwargs:
        raise ValueError('stdout argument not allowed, it will be overridden.')
    if 'stderr' in kwargs:
        raise ValueError('stderr argument not allowed, it will be overridden.')
    process = subprocess.Popen(stdout=subprocess.PIPE, stderr=subprocess.PIPE, *popenargs, **kwargs)
    output, stderr = process.communicate()
    retcode = process.poll()
    if retcode:
        cmd = kwargs.get("args")
        if cmd is None:
            cmd = popenargs[0]
        raise DetailedCalledProcessError(retcode, cmd, output=output.decode(), stderr=stderr.decode())
    return output.decode(), stderr.decode()


def convert_size_to_kb(size_str):
    if size_str.endswith('G'):
        return float(size_str[:-1]) * 1024 * 1024
    elif size_str.endswith('M'):
        return float(size_str[:-1]) * 1024
    elif size_str.endswith('K'):
        return float(size_str[:-1])
    else:
        return float(size_str) / 1024


def div(n, d):
    if d == 0.:
        return 1
    else:
        return n / d


def exit_process_group():
    """
    Remove a subprocess from its parent's process group.

    By default, subprocesses run within the same process group as the parent
    Python process that spawned them. Signals sent to the process group will be
    sent to the parent and also to to its children. Apparently SGE's qdel sends
    signals not to a process, but to its process group:

    https://community.oracle.com/thread/2335121

    Therefore, an inconveniently-timed SGE warning or other signal can thus be
    caught and handled both by Cosmos and the subprocesses it manages. Since
    Cosmos assumes all responsibility for job control when it starts a Task, if
    interrupted or signaled, we want to handle the event within Python
    exclusively. This method creates a new process group with only one member
    and thus insulates child processes from signals aimed at its parent.

    For more information, see these lecture notes from 1994:

    http://www.cs.ucsb.edu/~almeroth/classes/W99.276/assignment1/signals.html

    In particular:

    "One of the areas least-understood by most UNIX programmers is process-group
     management, a topic that is inseparable from signal-handling."

    "To make certain that no one could write an easily portable application,
     the POSIX committee added yet another signal handling environment which is
     supposed to be a superset of BSD and both System-V environments."

    "You must be careful under POSIX not to use the setpgrp() function --
     usually it exists, but performs the operation of setsid()."
    """
    return os.setsid()
