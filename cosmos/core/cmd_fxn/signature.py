
import decorator
import funcsigs


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


def default_prepend(task):  # pylint: disable=unused-argument
    """
    Set common error- and signal-handling behavior for Cosmos Tasks.

    set -e and set -o pipefail will cause Tasks that run multiple commands to error out at the
    first sign of failure, even if the failure occurs in a multiple-step pipe.

    the trap command is so that Tasks ignore three SGE signals that are handled by the Cosmos
    runtime (see commment on Workflow.py:SignalWatcher for more details).

    finally, one or two echo statements dump basic job/pid information to stderr.
    """
    bash_prelude = '#!/bin/bash\n' \
                   'set -e\n' \
                   'set -o pipefail\n' \
                   'trap \'\' USR1 USR2 XCPU\n' \
                   'echo "This task is running as pid $$ on ${HOSTNAME}" >&2\n' \
                   'echo "CWD is `pwd`" >&2\n'

    if task.drm == "ge":
        bash_prelude += 'echo "Managed by SGE: job ${JOB_ID}, ' \
            'submitted from ${SGE_O_LOGNAME}@${SGE_O_HOST}:${SGE_O_WORKDIR}" >&2\n'

    return bash_prelude + '\n'

# def default_cmd_append(task):
#     return ''


def default_cmd_fxn_wrapper(task, extra_prepend='', extra_append=''):
    """
    A default decorator that gets called each time a Task's command function is called.
    Generally useful for prepending/appending things to your commands.  Could also be used
    for automatically uploading/download inputs/outputs from an object store.
    """

    def real_decorator(fxn, *args, **kwargs):
        if getattr(fxn, 'skip_wrap', False):
            r = fxn(*args, **kwargs)
            return r
        else:
            r = fxn(*args, **kwargs)
            assert isinstance(r, basestring) or r is None, 'cmd_fxn %s did not return a str or None' % fxn
            if r is None:
                return None
            else:
                return default_prepend(task) + extra_prepend + r + extra_append

    return decorator.decorator(real_decorator)
