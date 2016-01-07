import re
from ... import NOOP
import funcsigs
import os


def call(cmd_fxn, task, input_map, output_map):
    sig = funcsigs.signature(cmd_fxn)

    def gen_params():
        for param_name, param in sig.parameters.iteritems():
            if param_name in input_map:
                yield param_name, input_map[param_name]
            elif param_name in output_map:
                yield param_name, output_map[param_name]
            elif param_name in task.tags:
                yield param_name, task.tags[param_name]
            elif param.default != funcsigs._empty:
                yield param_name, param.default
            else:
                raise AttributeError('%s requires the parameter `%s`, are you missing a tag?  Either provide a default in the cmd() '
                                     'method signature, or pass a value for `%s` with a tag' % (cmd_fxn, param_name, param_name))

    kwargs = dict(gen_params())

    out = cmd_fxn(**kwargs)

    for param_name in ['cpu_req', 'mem_req', 'drm']:
        if param_name in sig.parameters:
            param_val = kwargs.get(param_name, sig.parameters[param_name].default)
            setattr(task, param_name, param_val)

    assert isinstance(out, str) or out is None, 'cmd_fxn %s did not return a str or None' % cmd_fxn
    return out


import decorator


def default_prepend(execution_output_dir, task_output_dir):
    if task_output_dir and task_output_dir != '':
        task_output_dir = os.path.join(execution_output_dir, task_output_dir)
        mkdir = 'mkdir -p %s\n' % task_output_dir
    else:
        task_output_dir = execution_output_dir
        mkdir = ''

    return '#!/bin/bash\n' \
           'set -e\n' \
           'set -o pipefail\n' \
           'EXECUTION_OUTPUT_DIR={ex_out}\n' \
           '{mkdir}' \
           'cd {cd_to}\n\n'.format(ex_out=execution_output_dir,
                                   mkdir=mkdir,
                                   cd_to=task_output_dir)


# def default_cmd_append(task):
#     return ''


def default_cmd_fxn_wrapper(task, stage_name, input_map, output_map, *args, **kwargs):
    """
    WARNING this function signature is not set in stone yet and may change, replace at your own risk.

    :param task:
    :param input_map:
    :param output_map:
    :return:
    """

    def real_decorator(fxn, *args, **kwargs):
        r = fxn(*args, **kwargs)
        if r is None:
            return NOOP
        else:
            return default_prepend(task.execution.output_dir, task.output_dir) + r

    return decorator.decorator(real_decorator)
