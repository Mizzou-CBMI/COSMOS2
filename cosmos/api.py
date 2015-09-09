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
from .util.iterstuff import only_one

# from functools import wraps
# def partial(func, *args, **keywords):
#     """
#     Fuctionally the same as functools.partial, but uses functools.wraps to retain the signature of `func`
#     """
#     @wraps(func)
#     def newfunc(*fargs, **fkeywords):
#         newkeywords = keywords.copy()
#         newkeywords.update(fkeywords)
#         return func(*(args + fargs), **newkeywords)
#
#     return newfunc