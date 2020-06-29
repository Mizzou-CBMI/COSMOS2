import argparse
import os
import subprocess as sp
import sys
from functools import partial

from cosmos.api import (
    Cosmos,
    draw_stage_graph,
    draw_task_graph,
    pygraphviz_available,
    default_get_submit_args,
)


def echo(word):
    return f"echo {word}"


def recipe(workflow):
    # Create two Tasks that echo "hello" and "world" respectively (source nodes of the dag).
    echo_tasks = [
        workflow.add_task(func=echo, params=dict(word=word), uid=word) for word in list(map(str, range(1000)))
    ]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("-drm", default="local", help="", choices=("local", "drmaa:ge", "ge", "slurm"))
    p.add_argument("-j", "--job-class", help="Submit to this job class if the DRM supports it")
    p.add_argument("-q", "--queue", help="Submit to this queue if the DRM supports it")

    args = p.parse_args()

    cosmos = Cosmos(
        "sqlite:///%s/sqlite.db" % os.path.dirname(os.path.abspath(__file__)),
        # example of how to change arguments if you're not using default_drm='local'
        get_submit_args=partial(default_get_submit_args, parallel_env="smp"),
        default_drm=args.drm,
        default_max_attempts=2,
        default_job_class=args.job_class,
        default_queue=args.queue,
    )
    cosmos.initdb()

    sp.check_call("mkdir -p analysis_output/1000tasks/", shell=True)
    os.chdir("analysis_output/1000tasks/")

    workflow = cosmos.start("1000_tasks", restart=True, skip_confirm=True)

    recipe(workflow)

    workflow.make_output_dirs()
    workflow.run(max_cores=100)

    # Noting here that if you wanted to look at the outputs of any Tasks to decide how to generate the rest of a DAG
    # you can do so here, proceed to add more tasks via workflow.add_task(), and then call workflow.run() again.
    # Yes, it does require running all Tasks in the dag to get the outputs of any Task, and we hope to address
    # that limitation at some point in the future.

    if pygraphviz_available:
        # These images can also be seen on the fly in the web-interface
        draw_stage_graph(workflow.stage_graph(), "/tmp/ex1_task_graph.png", format="png")
        draw_task_graph(workflow.task_graph(), "/tmp/ex1_stage_graph.png", format="png")
    else:
        print("Pygraphviz is not available :(")

    sys.exit(0 if workflow.successful else 1)


if __name__ == "__main__":
    main()
