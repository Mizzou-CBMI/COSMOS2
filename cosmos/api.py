from . import *

from .core.cmd_fxn.io import find, out_dir, abstract_input_taskfile, abstract_output_taskfile, forward, abstract_output_taskfile_old
from .models.Task import Task
from .models.Stage import Stage
from .models.Tool import Tool, Input, Inputs
from .models.Execution import Execution
from .util.args import add_execution_args
from .util.tool import one2one, one2many, many2one, many2many, make_dict
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
