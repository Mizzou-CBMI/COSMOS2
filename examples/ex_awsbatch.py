import argparse
import os
import subprocess as sp
import sys

from cosmos.api import Cosmos


def get_instance_info(out_s3_uri, sleep=0):
    return f"""
    df -h > df.txt
    aws s3 cp df.txt {out_s3_uri}

    sleep {sleep}
    """


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument(
        "-i",
        "--container-image",
        help="the docker container image to use for the awsbatch job.  Note that "
        "the container_image must have bin/run_s3_script in its path (which "
        "will be true if cosmos is installed in the docker container).  This script is used to run "
        "a command script which was uploaded as a temporary file to s3.",
        required=True,
    )
    p.add_argument(
        "-b",
        "--s3-prefix-for-command-script-temp-files",
        help="Bucket to use for storing command scripts as temporary files, ex: s3://my-bucket/cosmos/tmp_files",
        required=True,
    )
    p.add_argument("-q", "--default-queue", help="aws batch queue", required=True)
    p.add_argument(
        "-o", "--out-s3-uri", help="s3 uri to store output of tasks", required=True,
    )
    p.add_argument("--core-req", help="number of cores to request for the job", default=1)
    p.add_argument(
        "--sleep",
        type=int,
        default=0,
        help="number of seconds to have the job sleep for.  Useful for debugging so "
        "that you can ssh into the instance running a task",
    )
    p.add_argument(
        "--retry-only-if-status-reason-matches",
        default="Host EC2 .+ terminated.",
        help="regular expression to match the task.staus_reason when deciding where to retry a Task.  This setting will "
        "only retry tasks that failed due to their instance being terminated, which happens frequently with spot "
        "instances ",
    )
    p.add_argument(
        "--max-attempts",
        default=2,
        help="Number of times to retry a task.  A task will only be retried if its status_reason matches the regex from"
        "--retry-only-if-status-reason-matches.",
    )
    return p.parse_args()


def main():
    args = parse_args()

    cosmos = Cosmos(
        "sqlite:///%s/sqlite.db" % os.path.dirname(os.path.abspath(__file__)),
        default_drm="awsbatch",
        default_drm_options=dict(
            container_image=args.container_image,
            s3_prefix_for_command_script_temp_files=args.s3_prefix_for_command_script_temp_files,
            retry_only_if_status_reason_matches=args.retry_only_if_status_reason_matches,
        ),
        default_queue=args.default_queue,
    )
    cosmos.initdb()

    sp.check_call("mkdir -p analysis_output/ex1", shell=True)
    os.chdir("analysis_output/ex1")
    workflow = cosmos.start("Example1", restart=True, skip_confirm=True)

    t = workflow.add_task(
        func=get_instance_info,
        params=dict(out_s3_uri=args.out_s3_uri, sleep=args.sleep),
        uid="",
        time_req=None,
        max_attempts=args.max_attempts,
        core_req=args.core_req,
        mem_req=1024,
    )
    workflow.run()

    print(("task.params", t.params))
    print(("task.input_map", t.input_map))
    print(("task.output_map", t.output_map))
    print(("task.core_req", t.core_req))
    print(("task.time_req", t.time_req))
    print(("task.drm", t.drm))
    print(("task.uid", t.uid))
    print(("task.drm_options", t.drm_options))
    print(("task.queue", t.queue))

    sys.exit(0 if workflow.successful else 1)


if __name__ == "__main__":
    main()
