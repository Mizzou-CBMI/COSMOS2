# from collections import namedtuple
# import operator
# from collections import OrderedDict
# import re
# from inspect import getargspec
# import funcsigs
# import re
# import itertools as it
# import os
# from ...util.helpers import make_dict
# import funcsigs
#
# FindFromParents = namedtuple('FindFromParents', 'regex n params')
# OutputDir = namedtuple('OutputDir', 'basename prepend_workflow_output_dir')
# Forward = namedtuple('Forward', 'input_parameter_name')
#
#
# def unpack_if_cardinality_1(find_instance, taskfiles):
#     op, number = parse_cardinality(find_instance.n)
#     if op in ['=', '=='] and number == 1:
#         return taskfiles[0]
#     else:
#         return taskfiles
#
#
# def _find(filenames, regex, error_if_missing=False):
#     found = False
#     for filename in filenames:
#         if re.search(regex, filename):
#             yield filename
#             found = True
#
#     if not found and error_if_missing:
#         raise ValueError, 'No taskfile found for %s' % regex
#
#
# OPS = OrderedDict([("<=", operator.le),
#                    ("<", operator.lt),
#                    (">=", operator.ge),
#                    (">", operator.gt),
#                    ('==', operator.eq),
#                    ("=", operator.eq)])
#
#
# def parse_cardinality(n):
#     try:
#         op, number = re.search('(.*?)(\d+)', str(n)).groups()
#     except AttributeError:
#         raise AttributeError('Invalid cardinality: %s' % n)
#     if op == '':
#         op = '=='
#     number = int(number)
#     return op, number
#
#
# def find(regex, n='==1', args=None):
#     """
#     Used to set an input_file's default behavior to finds output_files from a Task's parents using a regex.
#
#     :param str regex: a regex to match the file path.
#     :param str n: (cardinality) the number of files to find.
#     :param dict args: filter parent search space using these params.
#     """
#     return FindFromParents(regex, n, args)
#
#
# def out_dir(basename='', peo=True):
#     """
#     Essentially will perform os.path.join(Task.output_dir, basename)
#
#     :param str basename: The basename of the output_file.
#     :param bool peo: Prepend workflow.output_dir to the output path.
#     """
#     return OutputDir(basename, peo)
#
#
# def forward(input_parameter_name):
#     """
#     Forwards a Task's input as an output.
#
#     :param input_parameter_name: The name of this cmd_fxn's input parameter to forward.
#     """
#     return Forward(input_parameter_name)
#
#
# def _validate_input_mapping(cmd_name, param_name, find_instance, mapped_input_taskfiles, parents):
#     real_count = len(mapped_input_taskfiles)
#     op, number = parse_cardinality(find_instance.n)
#
#     if not OPS[op](real_count, int(number)):
#         import sys
#
#         print >> sys.stderr
#         print >> sys.stderr, '<ERROR msg="{cmd_name}() does not have right number of inputs for parameter ' \
#                              '`{param_name}` with default: {find_instance}, num_parents={num_parents}"'.format(num_parents=len(parents),
#                                                                                                                **locals())
#         for parent in parents:
#             print >> sys.stderr, '\t<PARENT task="%s">' % parent
#             if len(parent.output_files):
#                 for out_file in parent.output_files:
#                     print >> sys.stderr, '\t\t<OUTPUT_FILE path="%s" match=%s />' % (out_file, out_file in mapped_input_taskfiles)
#             print >> sys.stderr, '\t</PARENT>'
#         print >> sys.stderr, '</ERROR>'
#
#         raise ValueError('Input files are missing, or their cardinality do not match.')
#
#
# def _get_input_map(cmd_name, cmd_fxn, args, parents):
#     # todo handle inputs without default
#
#     sig = funcsigs.signature(cmd_fxn)
#
#     # funcsigs._empty
#     for param_name, param in sig.parameters.iteritems():
#         if isinstance(param.default, FindFromParents):
#             assert param_name.startswith('in_'), 'Input parameter names must start with out_'
#
#         if param_name.startswith('in_'):
#             value = args.get(param_name, param.default)
#             if value == funcsigs._empty:
#                 raise ValueError, 'Required input file parameter `%s` not specified for `%s`' % (param_name, cmd_name)
#             elif isinstance(value, FindFromParents):
#                 # user used find()
#                 find_instance = value
#
#                 def get_available_files():
#                     for p in parents:
#                         if all(p.args.get(k) == v for k, v in (find_instance.args or dict()).items()):
#                             yield p.output_files
#
#                 available_files = it.chain(*get_available_files())
#                 input_taskfiles = list(_find(available_files, find_instance.regex, error_if_missing=False))
#                 _validate_input_mapping(cmd_name, param_name, find_instance, input_taskfiles, parents)
#                 input_taskfile_or_input_taskfiles = unpack_if_cardinality_1(find_instance, input_taskfiles)
#
#                 yield param_name, input_taskfile_or_input_taskfiles
#             elif isinstance(value, basestring):
#                 # allows a user to remove an input_file by passing None for its value
#                 yield param_name, value
#
#
# def _get_output_map(stage_name, cmd_fxn, args, input_map, task_output_dir, workflow_output_dir):
#     sig = funcsigs.signature(cmd_fxn)
#
#     for param_name, param in sig.parameters.iteritems():
#         if isinstance(param.default, OutputDir):
#             assert param_name.startswith('out_'), 'Output parameter names must start with out_'
#
#         if param_name.startswith('out_'):
#             value = args.get(param_name, param.default)
#
#             if value == funcsigs._empty:
#                 raise ValueError('Required output file parameter `%s` not specified for %s.' % (param_name, stage_name))
#             elif isinstance(value, Forward):
#                 forward_instance = value
#                 try:
#                     input_value = input_map[forward_instance.input_parameter_name]
#                 except KeyError:
#                     raise KeyError('Cannot forward name `%s`,it is not a valid input parameter of '
#                                    '%s in stage %s' % (forward_instance.input_parameter_name, cmd_fxn, stage_name))
#                 yield param_name, input_value
#             elif isinstance(value, OutputDir):
#                 output_dir_instance = value
#                 if task_output_dir is not None:
#                     if output_dir_instance.prepend_workflow_output_dir:
#                         task_output_dir = os.path.join(workflow_output_dir, task_output_dir)
#                     output_file = os.path.join(task_output_dir, output_dir_instance.basename)
#
#                 else:
#                     output_file = output_dir_instance.basename
#                 # output_file = value.format(**params)
#                 yield param_name, output_file.format(**make_dict(input_map, args))
#             elif isinstance(value, basestring):
#                 # allows a user to remove an output_file by passing None for its value
#                 yield param_name, value.format(**args)
#
#
# def get_io_map(fxn, args, parents, cmd_name, task_output_dir, workflow_output_dir):
#     # input_arg_to_default, output_arg_to_default = get_input_and_output_defaults(fxn)
#     input_map = dict(_get_input_map(cmd_name, fxn, args, parents))
#     output_map = dict(_get_output_map(cmd_name, fxn, args, input_map, task_output_dir, workflow_output_dir))
#
#     return input_map, output_map
#
#
# def unpack_io_map(io_map):
#     return list(it.chain(*(v if isinstance(v, list) else [v] for v in io_map.values())))
