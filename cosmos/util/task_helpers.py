import os

import funcsigs
from decorator import decorator

import re


def bucket_url_and_key(gs_path):
    m = re.search('(gs://.+?)/.+$', gs_path)
    if m:
        gs_bucket_path = m.group(1)
        key = gs_path.replace(gs_bucket_path + '/', '')
        return gs_bucket_path, key
    else:
        raise ValueError('Cannot split bucket path: %s' % gs_path)


def bucket_and_key(gs_path):
    bucket_url, key = bucket_url_and_key(gs_path)
    return re.sub('^gs://', '', bucket_url), key


def stage_to_scratch(*args, gsutil_cmd='gsutil', parallel_cmd='parallel', exclude=None):
    """
    func is a Task function which returns a string that will later get submitted by Cosmos as a bash command.
    This is a decorator which sandwiches the return of func with more bash code that will setup scratch space and
    pull/push input and output files from google storage into/out of scratch.

    Input parameter names must start with `in_` and outputs must start with `out_`.
    Directories must end with _dir (ie in_x_dir and out_y_dir)

    In more detail:
     * Creates and cds into a temporary directory in /scratch
     * Downloads input gs:// files into /scratch
     * Calls func, passing in /scratch versions of input and output parameters instead of the gs:// locations
     * Uploads the /scratch files up to gs://
     * Cleans up scratch

    Optionally, this decorator can take a list of in/out parameters to exclude, which will exclude these parameters
    from being staged.

    ex:  `stage_to_scratch(exclude=['in_bam'])(task_func)`
    """
    if exclude is None:
        exclude = []

    @decorator
    def _stage_to_scratch(func, *args, **kwargs):

        sig = funcsigs.signature(func)
        in_params = dict(zip(sig.parameters.keys(), args))
        passthru_params = in_params.copy()

        stage_downs = []
        stage_ups = []

        for param_name, param_val in in_params.items():
            is_input = param_name.startswith('in_') and param_name not in exclude
            is_output = param_name.startswith('out_') and param_name not in exclude
            is_dir = param_name.endswith('_dir') and param_name not in exclude

            def stage_file_if_necessary(file_path_or_paths):
                if file_path_or_paths is None:
                    return None
                elif isinstance(file_path_or_paths, list):
                    return [stage_file_if_necessary(p) for p in file_path_or_paths]
                elif isinstance(file_path_or_paths, tuple):
                    return tuple(stage_file_if_necessary(p) for p in file_path_or_paths)
                elif isinstance(file_path_or_paths, dict):
                    return {k: stage_file_if_necessary(p) for k, p in file_path_or_paths.items()}
                elif isinstance(file_path_or_paths, str):
                    if file_path_or_paths.startswith('gs://'):
                        gs_bucket_path, stage_path = bucket_url_and_key(file_path_or_paths)

                        if is_input:
                            stage_downs.append((is_dir,
                                                os.path.join(gs_bucket_path, stage_path),
                                                stage_path))
                        elif is_output:
                            stage_ups.append((is_dir,
                                              stage_path,
                                              os.path.join(gs_bucket_path, stage_path)))
                        return stage_path
                return file_path_or_paths

            if is_input or is_output:
                passthru_params[param_name] = stage_file_if_necessary(param_val)

        # Assemble bash commands to stage necessary files and execute the given command.
        def mkdir_cmd(dirs):
            return [f"mkdir -p {dirname}" for dirname in sorted(set(dirs)) if dirname]

        def stage_cmd(stages):
            def gen_stage_cmds():
                for is_dir, from_, to_ in stages:
                    to_ = os.path.dirname(to_) if is_dir else to_
                    args = '-r ' if is_dir else ''
                    yield f'{gsutil_cmd} -mq cp {args}"{from_}" "{to_}"'

            # note that this expects gnu-parallel rather than the parallel installed with more-utils
            # gnu-parallel is just parallel in bioconda
            return [f"\ntime {parallel_cmd} -j {max(len(stages), 15)} --link <<EOF"] + \
                   [f"  {cmd}" for cmd in gen_stage_cmds()] + ["EOF"]

        setup_cmd = """
###### SETUP ######
export GCS_OAUTH_TOKEN=`gcloud auth application-default print-access-token`


## Setup the scratch directory

# Parameter expansion checking to see if SLURM_SCRATCH_DIR is set
# https://stackoverflow.com/questions/3601515/how-to-check-if-a-variable-is-set-in-bash
if [ -z ${SLURM_SCRATCH_DIR+x} ]
then
    export SCRATCH=`mktemp -d /scratch/align-XXXXXXXX`
else
    export SCRATCH="$SLURM_SCRATCH_DIR"
fi
export TMPDIR="$SCRATCH"
export _JAVA_OPTIONS="-Djava.io.tmpdir=$SCRATCH"
pushd .
cd "$SCRATCH"

echo cwd: `pwd`
"""
        prepend_cmds = []
        append_cmds = []
        if stage_downs:
            prepend_cmds += ['###### STAGE DOWN ######']
            prepend_cmds += mkdir_cmd([p if is_dir else os.path.dirname(p) for is_dir, _, p in stage_downs])
            prepend_cmds += stage_cmd(stage_downs)

        if stage_ups:
            # If we need to upload, create directories for the outputs before the command runs
            prepend_cmds += ['\n# Create scratch output dirs:']
            prepend_cmds += mkdir_cmd([p if is_dir else os.path.dirname(p) for is_dir, p, _ in stage_ups])

            append_cmds += ["###### STAGE UP ######"]
            append_cmds += stage_cmd(stage_ups)

        append_cmds.append("""
## Clenaup the scratch directory

popd
if [ -z ${SLURM_SCRATCH_DIR+x} ]
then
    # Only need to clean up the directory if not running inside SLURM;
    # otherwise the task epilog script will do it for us
    rm -rf "$SCRATCH"
fi
""")

        func_cmd = f"\n###### COMMAND ######\n{func(**passthru_params)}"

        def njoin(*args):
            return '\n'.join(args)

        return njoin(setup_cmd, njoin(*prepend_cmds), func_cmd, njoin(*append_cmds))

    if len(args) == 1 and callable(args[0]):
        # No arguments, this is the decorator
        # Set default values for the arguments
        return _stage_to_scratch(*args)
    else:
        # This is just returning the decorator
        assert len(args) == 0, "You can only specify the `exclude` kwarg when passing parameters to this decorator"
        return _stage_to_scratch
