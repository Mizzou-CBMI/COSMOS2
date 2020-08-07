import signal


TERMINATION_SIGNALS = frozenset({signal.SIGINT, signal.SIGTERM, signal.SIGXCPU})
