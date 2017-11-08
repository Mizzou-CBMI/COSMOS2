# from .core.cmd_fxn.io import find, out_dir, forward
from cosmos.core.cmd_fxn.signature import default_cmd_fxn_wrapper
from cosmos.models.Cosmos import Cosmos, default_get_submit_args
from cosmos.models.Task import Task
from cosmos.models.Stage import Stage
from cosmos.models.Workflow import Workflow, default_task_log_output_dir
from cosmos import WorkflowStatus, StageStatus, TaskStatus, NOOP, signal_workflow_status_change, signal_stage_status_change, signal_task_status_change, \
    Dependency

from cosmos.util.args import add_workflow_args
from cosmos.util.helpers import make_dict
from cosmos.util.iterstuff import only_one
from cosmos.util.signal_handlers import SGESignalHandler, handle_sge_signals

from cosmos.graph.draw import draw_task_graph, draw_stage_graph, pygraphviz_available
import funcsigs
import re

from decorator import decorator
import contextlib
import os


def load_input(out_file): pass


def arg_to_str(name, value):
    if value is None:
        return ''
    if isinstance(value, bool):
        return name if value else ''
    else:
        return '%s %s' % (name, value)


def args_to_str(*args):
    """
    Turn a set of arguments into a string to be passed to a command line tool

    If value is None or False it will be ignored.
    If value is True, emit --{arg_flag} without specifing a value.
    Otherwise emit --{arg_flag} {value}.

    :param args: An iterable of (str arg_flag, value) tuples.

    >>> x = 'bar'
    >>> y = None
    >>> z = 123
    >>> f = True
    >>> args_to_str(('--foo', x))
    '--foo bar'
    >>> args_to_str(('--flag', f))
    '--flag'
    >>> args_to_str(('--skip-me', y), ('--use-me', z))
    '--use-me 123'
    """
    return " \\\n".join(arg_to_str(k, v) for k, v in args if arg_to_str(k, v) != '')


@contextlib.contextmanager
def cd(path):
    """
    A context manager which changes the working directory to the given
    path, and then changes it back to its previous value on exit.
    """
    prev_cwd = os.getcwd()
    os.chdir(path)
    yield
    os.chdir(prev_cwd)


@decorator
def bash_call(func, *args, **kwargs):
    """
    A function decorator which provides a way to avoid writing boilerplate argparse code when defining a Task.
    It converts the decorated function to produce a bash script that uses python to import the function and
    call it with the same arguments.

    Current Limitations:
       * Function must be importable
       * No partials


    >>> def echo(arg1, out_file='out.txt'):
    ...     with open(out_file) as fp:
    ...         print(arg1, file=fp)
    >>> print(bash_call(echo)(arg1='hello world'))
    <BLANKLINE>
    python - <<EOF
    <BLANKLINE>
    try:
        from cosmos.api import echo
    except ImportError:
        import imp
        echo = imp.load_source('echo', 'None').echo
    <BLANKLINE>
    echo(**
    {'arg1': 'hello world',
     'out_file': 'out.txt'}
    )
    <BLANKLINE>
    EOF
    """

    import pprint
    import inspect

    sig = funcsigs.signature(func)
    kwargs = dict(zip(sig.parameters.keys(), args))

    return r"""
python - <<EOF

try:
    from {func.__module__} import {func.__name__}
except ImportError:
    import imp
    {func.__name__} = imp.load_source('{module_name}', '{source_file}').{func.__name__}

{func.__name__}(**
{param_str}
)

EOF""".format(func=func,
              module_name=func.__name__,
              source_file=inspect.getsourcefile(func),
              param_str=pprint.pformat(kwargs, width=1, indent=1))

