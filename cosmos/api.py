from . import *

from .core.cmd_fxn.io import find, out_dir, forward
from . import Cosmos

from .models.Task import Task
from .models.Stage import Stage
from .models.Execution import Execution

from .util.args import add_execution_args
from .util.relationship_patterns import one2one, one2many, many2one, group
from .util.helpers import make_dict
from .util.iterstuff import only_one

from .graph.draw import draw_task_graph, draw_stage_graph, pygraphviz_available
from black_magic.decorator import partial
from decorator import decorator

def load_input(in_file, out_file=forward('in_file')):
    return NOOP


import funcsigs


@decorator
def stringify(func, *args, **kwargs):
    """
    Experimental way to not have to write boiler plate argparse code.  Code is still run under a completely separate process

    Current Limitations:
       * function must be importable from anywhere in the VE
          * This means no partials!!! :( Parameters must all be passed as tags
    """

    # decorator actually only gets args, use function signature to turn it into kwargs which is more explicit
    def gen_kwargs_str():
        sig = funcsigs.signature(func)
        for k, v in zip(sig.parameters.keys(), args):
            if isinstance(v, basestring):
                v = '"%s"' % v
            yield '%s=%s' % (k, v)

    return r"""

python - <<EOF
from {func.__module__} import {func.__name__}

{func.__name__}({param_str})

EOF""".format(func=func,
               param_str=','.join(gen_kwargs_str()))  # todo assert values are basetypes
