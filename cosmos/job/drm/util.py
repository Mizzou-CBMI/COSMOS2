import os
import sys
from cosmos.util.signal_handlers import sleep_through_signals

if os.name == "posix" and sys.version_info[0] < 3:
    import subprocess32 as subprocess
else:
    import subprocess


def convert_size_to_kb(size_str):
    if size_str.endswith("G"):
        return float(size_str[:-1]) * 1024 * 1024
    elif size_str.endswith("M"):
        return float(size_str[:-1]) * 1024
    elif size_str.endswith("K"):
        return float(size_str[:-1])
    else:
        return float(size_str) / 1024


def div(n, d):
    if d == 0.0:
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


def run_cli_cmd(
    args,
    attempts=1,
    interval=15,
    logger=None,
    preexec_fn=exit_process_group,
    timeout=30,
    trust_exit_code=True,
    **kwargs
):
    """
    Run the supplied cmd, optionally retrying some number of times if it fails or times out.

    You can pass through arbitrary arguments to this command. They eventually
    wind up as constructor arguments to subprocess.Popen().
    """
    while attempts > 0:
        attempts -= 1
        try:
            result = subprocess.run(
                args,
                check=True,
                stderr=subprocess.PIPE,
                stdout=subprocess.PIPE,
                timeout=timeout,
                universal_newlines=True,
                **kwargs
            )
            if result.returncode == 0:
                if trust_exit_code:
                    attempts = 0
                elif result.stdout:
                    attempts = 0
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
            result = exc

        if logger is not None:
            log_func = logger.error
            details = ": stdout='%s', stderr='%s'" % (
                result.stdout.strip(),
                result.stderr.strip(),
            )
            if isinstance(result, subprocess.TimeoutExpired):
                effect = "exceeded %s-sec timeout" % result.timeout
            else:
                effect = "had exit code %s" % result.returncode
                if result.returncode == 0 and attempts == 0:
                    log_func = logger.debug
                    details = ""

            plan = "will retry in %s sec" % interval if attempts else "final attempt"
            log_func(
                "Call to %s %s (%s)%s",
                args.split()[0] if isinstance(args, str) else args[0],
                effect,
                plan,
                details,
            )

        if attempts:
            sleep_through_signals(timeout=interval)

    returncode = result.returncode if hasattr(result, "returncode") else "TIMEOUT"
    return result.stdout, result.stderr, returncode
