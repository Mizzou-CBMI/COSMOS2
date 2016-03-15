from .core.cmd_fxn.io import find, out_dir, forward
from .core.cmd_fxn.signature import default_cmd_fxn_wrapper
from .models.Cosmos import Cosmos, default_get_submit_args
from .models.Task import Task
from .models.Stage import Stage
from .models.Execution import Execution
from . import ExecutionStatus, StageStatus, TaskStatus, NOOP, signal_execution_status_change, signal_stage_status_change, signal_task_status_change

from .util.args import add_execution_args
from .util.relationship_patterns import one2one, one2many, many2one, many2many, group
from .util.helpers import make_dict
from .util.iterstuff import only_one

from .graph.draw import draw_task_graph, draw_stage_graph, pygraphviz_available
import funcsigs

from black_magic.decorator import partial
from decorator import decorator


def load_input(in_file, out_file=forward('in_file')): pass


def load_inputs(in_files, out_files=forward('in_files')): pass


def arg(name, value):
    if value:
        if value == True:
            return name
        else:
            return '%s %s' % (name, value)
    else:
        return ''


def args(*args):
    return " \\\n".join(arg(k, v) for k, v in args if arg(k, v) != '')


@decorator
def bash_call(func, *args, **kwargs):
    """
    A function decorator which provides a way to avoid writing boilerplate argparse code when defining a Task. Converts any function call to a bash script
    that uses python to import the function and call it with the same arguments.

    Current Limitations:
       * function must be importable from anywhere in the VE
       * This means no partials! So parameters must all be passed as tags.


    def echo(arg1, out_file=out_dir('out.txt')):
        with open(out_file) as fp:
            print >> fp, arg1

    bash_call(echo)(arg1='hello world')
    python - <<EOF

    from my_module import echo

    echo(** {'arg1': 'hello world',
             'out_file': OutputDir(basename='out.txt', prepend_execution_output_dir=True)}

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
