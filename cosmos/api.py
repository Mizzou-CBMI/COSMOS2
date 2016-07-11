# from .core.cmd_fxn.io import find, out_dir, forward
from .core.cmd_fxn.signature import default_cmd_fxn_wrapper
from .models.Cosmos import Cosmos, default_get_submit_args
from .models.Task import Task
from .models.Stage import Stage
from .models.Workflow import Workflow, default_task_log_output_dir
from . import WorkflowStatus, StageStatus, TaskStatus, NOOP, signal_workflow_status_change, signal_stage_status_change, signal_task_status_change, Dependency

from .util.args import add_workflow_args
from .util.relationship_patterns import group
from .util.helpers import make_dict
from .util.iterstuff import only_one

from .graph.draw import draw_task_graph, draw_stage_graph, pygraphviz_available
import funcsigs
import re

from black_magic.decorator import partial
from decorator import decorator


# from cosmos.core.cmd_fxn.io import _validate_input_mapping, unpack_if_cardinality_1


# def load_input(in_file, out_file=forward('in_file')): pass
# def load_inputs(in_files, out_files=forward('in_files')): pass

def load_input(out_file): pass


def arg_to_str(name, value):
    if value:
        if value == True:
            return name
        else:
            return '%s %s' % (name, value)
    else:
        return ''


def args_to_str(*args):
    """
    Turn a set of arguments into a string to be passed to a command line tool

    :param args: A number of (str arg_flag, value) tuples.  If value is None or False it will be ignored.  Otherwise produce --{arg_flag} {value}.

    >>> x = 'bar'
    >>> y = None
    >>> z = 123
    >>> args_to_str(('--foo', x))
    '--foo bar'

    >>> args_to_str(('--skip-me', y), ('--use-me', z))
    '--use-me 123'

    """
    return " \\\n".join(arg_to_str(k, v) for k, v in args if arg_to_str(k, v) != '')

# arg = _arg_to_str
# args = args_to_str

import contextlib
import os


@contextlib.contextmanager
def cd(path):
    """A context manager which changes the working directory to the given
    path, and then changes it back to its previous value on exit.

    """
    prev_cwd = os.getcwd()
    os.chdir(path)
    yield
    os.chdir(prev_cwd)


# def find2(regex, parents, n='==1'):
#     if isinstance(parents, Task):
#         parents = [parents]
#     g = (file_path for p in parents for file_path in p.output_files)
#     files = [file_path for file_path in g if re.search(regex, file_path)]
#     # validate cardinality and unpack...
#     _validate_input_mapping('cmd?', 'param?', find(regex,n), files, parents)
#     return unpack_if_cardinality_1(find(regex, n), files)



# def bash_call(func):
#     func.bash_call = True
#     return func

@decorator
def bash_call(func, *args, **kwargs):
    """
    A function decorator which provides a way to avoid writing boilerplate argparse code when defining a Task. Converts any function call to a bash script
    that uses python to import the function and call it with the same arguments.

    Current Limitations:
       * function must be importable from anywhere in the VE
       * This means no partials! So parameters must all be passed as params.


    def echo(arg1, out_file=out_dir('out.txt')):
        with open(out_file) as fp:
            print >> fp, arg1

    bash_call(echo)(arg1='hello world')
    python - <<EOF

    from my_module import echo

    echo(** {'arg1': 'hello world',
             'out_file': OutputDir(basename='out.txt', prepend_workflow_output_dir=True)}

    EOF

    """

    import pprint

    sig = funcsigs.signature(func)
    kwargs = dict(zip(sig.parameters.keys(), args))

    return r"""

python - <<EOF

from {func.__module__} import {func.__name__}

{func.__name__}(**
{param_str}
)

EOF""".format(func=func,
              param_str=pprint.pformat(kwargs, width=1, indent=1))  # todo assert values are basetypes

@decorator
def run(func, *args, **kwargs):
    """
    Similar to bash_call, but actually just returns a string that is the source code of this function instead of importing it.
    """
    import inspect
    import pprint

    source = inspect.getsource(func)

    sig = funcsigs.signature(func)
    kwargs = dict(zip(sig.parameters.keys(), args))

    return r"""

python - <<EOF

{soource}

{func.__name__}(**
{param_str}
)

EOF""".format(func=func,
              source=source,
              param_str=pprint.pformat(kwargs, width=1, indent=1))
