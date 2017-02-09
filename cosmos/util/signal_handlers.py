"""
Functions and classes useful for handling signals in an SGE environment.

Depending on its configuration, SGE can send a SIGUSR1, SIGUSR2, and/or SIGXCPU
before sending SIGSTOP/SIGKILL signals (which cannot be caught).

According to ``man qsub`` (search for -notify), SGE can:

> send "warning" signals to a running job prior to sending the signals
> themselves. If a SIGSTOP is pending, the job will receive a SIGUSR1 several
> seconds before the SIGSTOP. If a SIGKILL is pending, the job will receive a
> SIGUSR2 several seconds before the SIGKILL.

In addition, according to ``man queue_conf``:

> If s_cpu is exceeded, the job is sent a SIGXCPU signal which can be caught by
> the job.... If s_vmem is exceeded, the job is sent a SIGXCPU signal which can
> be caught by the job.... If s_rss is exceeded, the job is sent a SIGXCPU
> signal which can be caught by the job....

Unfortunately, SIGUSR1 can mean two different things. As above, when -notify is
set, it means a SIGSTOP is imminent. However, and less frequently, it can also
mean a job is about to be killed. See, again, ``man queue_conf``:

> If h_rt is exceeded by a job running in the queue, it is aborted via the
> SIGKILL signal (see kill(1)). If s_rt is exceeded, the job is first "warned"
> via the SIGUSR1 signal (which can be caught by the job) and finally aborted
> after the notification time defined in the queue configuration parameter
> notify (see above) has passed.

SIGUSR1 signals, then, are more likely than not to be a benign indication
that a job is about to be paused (since timeouts are far rarer than pauses).
However, when one is caught, we log that it may be a sign the job is about
to die.
"""

import collections
import signal
import sys
import threading
import time


def die(signum, frame):    # pylint: disable=unused-argument
    """
    Immediately exit and set the error code to the signal number received.
    """
    sys.exit(signum)


def handle_sge_signals():
    """
    Respond to SGE signals simply, until a SignalWrapper is ready to handle them.
    """
    signal.signal(signal.SIGUSR1, signal.SIG_IGN)   # SIGSTOP (probably) is coming, ignore
    signal.signal(signal.SIGUSR2, die)              # SIGKILL is coming, die
    signal.signal(signal.SIGXCPU, die)              # SIGKILL is coming, die


def sleep_through_signals(timeout):
    """
    If time.sleep() is interrupted by a signal, go back to sleep until timeout is exceeded.
    """
    start_tm = time.time()
    elapsed_tm = time.time() - start_tm
    while elapsed_tm < timeout:
        time.sleep(timeout - elapsed_tm)
        elapsed_tm = time.time() - start_tm


class SGESignalHandler(object):
    """
    Monitors signals and sets a flag on the workflow when a fatal one is caught.

    Default responses and explanations are per an out-of-the-box SGE
    installation, but these can be configured with constructor parameters.

    Easiest way to use this class is to wrap a call to run() in a with-statement:

        def main():
            handle_sge_signals()
            ...
            # create a dag and workflow, etc.
            ...
            with SGESignalHandler(workflow):
                workflow.run()
    """

    def __init__(
        self, workflow,
        lethal_signals=frozenset([signal.SIGINT, signal.SIGTERM,
                                  signal.SIGUSR2, signal.SIGXCPU]),
        benign_signals=frozenset([signal.SIGUSR1]),
        explanations={
            signal.SIGUSR1: 'SGE is about to send a SIGSTOP, or, '
                            'if a time limit has been exceeded, a SIGKILL',
            signal.SIGUSR2: 'SGE is about to send a SIGKILL',
            signal.SIGXCPU: 'SGE is about to send a SIGKILL, '
                            'because a cpu resource limit has been exceeded'}):

        self.workflow = workflow
        self.lethal_signals = lethal_signals
        self.benign_signals = benign_signals
        self.explanations = explanations

        self._prev_handlers = dict()
        self._signals_caught = collections.Counter()
        self._signals_logged = collections.Counter()

        self._logging_enabled = False
        self._logging_event = None
        self._logging_daemon = None

        #
        # SQLAlchemy prohibits accessing these properties from the logging thread,
        # so let's cache them when we know we're running on the main thread.
        #
        self._log = self.workflow.log
        self._workflow_name = str(self.workflow)

    def __enter__(self):
        self._logging_enabled = True
        self._logging_event = threading.Event()
        self._logging_daemon = threading.Thread(target=self.logging_daemon)
        self._logging_daemon.daemon = True

        self._logging_daemon.start()

        for sig in self.lethal_signals | self.benign_signals:
            self._cache_existing_handler(sig)
            signal.signal(sig, self.signal_handler)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        for sig, handler in self._prev_handlers.items():
            signal.signal(sig, handler)

        self._prev_handlers.clear()

        self._logging_enabled = False
        self._logging_event.set()
        self._logging_daemon.join(timeout=1)

        self._log.info('%s Caught/processed %d/%d signal(s) while running',
                       self._workflow_name,
                       sum(self._signals_caught.values()),
                       sum(self._signals_logged.values()))

    def signal_handler(self, signum, frame):    # pylint: disable=unused-argument
        self._signals_caught[signum] += 1
        self._logging_event.set()

        if signum in self.lethal_signals:
            self.workflow.terminate_when_safe = True

    def _cache_existing_handler(self, sig):
        prev_handler = signal.getsignal(sig)
        if prev_handler not in (die, signal.SIG_DFL, signal.SIG_IGN, signal.default_int_handler):
            raise RuntimeError(
                "a signal handler is already set for signal %d (%s): %s" %
                (sig, self._explain(sig), prev_handler))
        self._prev_handlers[sig] = prev_handler

    def _explain(self, signum):
        names = []
        for k, v in signal.__dict__.iteritems():
            if k.startswith('SIG') and v == signum:
                names.append(k)
        names.sort()

        if signum in self.explanations:
            return ': '.join((' or '.join(names), self.explanations[signum]))
        else:
            return ' or '.join(names)

    def _log_signal_receipt(self, signal_counter):
        for sig, cnt in signal_counter.items():
            self._log.info('%s Caught signal %d %s(%s)',
                           self._workflow_name, sig,
                           '%d times ' % cnt if cnt > 1 else '',
                           self._explain(sig))

    def logging_daemon(self):
        while self._logging_enabled:
            self._logging_event.wait()
            new_signals = self._signals_caught - self._signals_logged
            self._logging_event.clear()

            if new_signals:
                self._log_signal_receipt(new_signals)
                self._signals_logged += new_signals

                if self.workflow.terminate_when_safe:
                    self._log.info('%s Early-termination flag has been set',
                                   self._workflow_name)
                else:
                    self._log.debug('%s Ignoring benign signal(s)', self._workflow_name)
