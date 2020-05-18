"""
py_call_cmd_wrapper allows you to use pure python rather than bash scripts.  At runtime, Cosmos will create a
python script which imports the task function and calls it with the appropriate arguments.  Note you can still use
subprocess.run to call executables (like tools created by third parties).
"""

import os

from cosmos.api import Cosmos, py_call_cmd_wrapper


def say(text, out_file):
    with open(out_file, "w") as fp:
        print(text, file=fp)


if __name__ == "__main__":
    cosmos = Cosmos(
        "sqlite:///%s/sqlite.db" % os.path.dirname(os.path.abspath(__file__)),
        default_drm="local",
    )
    cosmos.initdb()
    workflow = cosmos.start("Example3", restart=True, skip_confirm=True)

    t = workflow.add_task(
        func=say, params=dict(text="Hello World", out_file="out.txt"), uid="my_task"
    )

    workflow.make_output_dirs()
    workflow.run(cmd_wrapper=py_call_cmd_wrapper)
