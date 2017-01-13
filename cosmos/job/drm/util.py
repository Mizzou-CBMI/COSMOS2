import os


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


def preexec_function():
    """
    Prevent signals from cascading to subprocesses.

    By default, subprocesses run within the same process group as the parent
    Python process that ran them. Signals sent to the parent will also be sent
    to its children; a poorly-timed ctrl+c interrupt or other signal can thusly
    be caught and handled both by Python and the SGE tools used in this module.

    If interrupted or signaled, we want to handle the event within Python
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
