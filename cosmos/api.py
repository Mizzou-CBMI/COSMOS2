# from .core.cmd_fxn.io import find, out_dir, forward
from .core.cmd_fxn.signature import default_cmd_fxn_wrapper
from .models.Cosmos import Cosmos, default_get_submit_args
from .models.Task import Task
from .models.Stage import Stage
from .models.Workflow import Workflow
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



def bash_call(func):
    func.bash_call = True
    return func



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
