import re
from ... import NOOP
import funcsigs
import os


def get_call_kwargs(cmd_fxn, params, input_map, output_map):
    sig = funcsigs.signature(cmd_fxn)

    def gen_params():
        for keyword, param in sig.parameters.iteritems():
            if keyword in input_map:
                yield keyword, input_map[keyword]
            elif keyword in output_map:
                yield keyword, output_map[keyword]
            elif keyword in params:
                yield keyword, params[keyword]
            elif param.default != funcsigs._empty:
                yield keyword, param.default
            else:
                raise AttributeError(
                        '%s requires the parameter `%s`, are you missing a tag?  Either provide a default in the cmd() '
                        'method signature, or pass a value for `%s` with a tag' % (cmd_fxn, keyword, keyword))

    #TODO dont format with params?
    kwargs = {k: v.format(**params) if isinstance(v, basestring) else v for k, v in gen_params()}
    return kwargs


import decorator


def default_prepend(task):  # pylint: disable=unused-argument
    """
    Set common error- and signal-handling behavior for Cosmos Tasks.

    set -e and set -o will cause Tasks that run multiple commands to error out at the
    first sign of failure, even if the failure occurs in a multiple-step pipe.

    the trap command is so that Tasks ignore three SGE signals that are handled by the Cosmos
    runtime (see commment on Workflow.py:SignalWatcher for more details).
    """
    return '#!/bin/bash\n' \
           'set -e\n' \
           'set -o pipefail\n' \
           'trap \'\' USR1 USR2 XCPU\n' \
           '\n'

# def default_cmd_append(task):
#     return ''


def default_cmd_fxn_wrapper(task, stage_name, input_map, output_map, cd_to_task_output_dir=True):
    """
    WARNING this function signature is not set in stone yet and may change, replace at your own risk.

    :param task:
    :param input_map:
    :param output_map:
    :return:
    """

    def real_decorator(fxn, *args, **kwargs):
        r = fxn(*args, **kwargs)
        assert isinstance(r, basestring) or r is None, 'cmd_fxn %s did not return a str or None' % fxn
        if r is None:
            return None
        else:
            return default_prepend(task) + r

    return decorator.decorator(real_decorator)
