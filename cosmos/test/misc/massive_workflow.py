import time

from cosmos.api import Cosmos, default_get_submit_args
from cosmos.util.signal_handlers import SGESignalHandler, handle_sge_signals
import os
from functools import partial


def noop(out_fn):
    time.sleep(0.01)
    return "ls"


def silly_recipe(wf, i, num_things):
    prev_task = None
    for j in range(num_things):
        prev_task = wf.add_task(
            func=noop,
            parents=[] if prev_task is None else [prev_task],
            params={"out_fn": "{}/{}.txt".format(i, j)},
            uid="{}/{}".format(i, j),
        )


def main():
    # start cosmos engine
    cosmos = Cosmos(
        database_url="sqlite://",
        default_drm="local",
        # default_drm="ge",
        default_queue="dev-short",
        default_drm_options={},
        get_submit_args=partial(default_get_submit_args, parallel_env="smp"),
    )
    cosmos.initdb()

    # create cosmos workflow
    workflow = cosmos.start(
        # NOTE cosmos will make dirs in this path
        # primary_log_path=os.path.join("logs", "cosmos.log"),
        name="blah",
        restart=True,
        skip_confirm=True,
        fail_fast=True,
    )

    for i in range(100):
        print("add {}".format(i))
        silly_recipe(workflow, i, 100)

    workflow.make_output_dirs()

    # run cosmos workflow
    # with SGESignalHandler(workflow):
    workflow.run()


if __name__ == "__main__":
    main()
