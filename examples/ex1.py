import sys

from cosmos.api import Cosmos


def say(text, out_file):
    with open(out_file, "w") as fp:
        fp.write(text)


def main():
    cosmos = Cosmos("cosmos.sqlite").initdb()

    workflow = cosmos.start("ex1", skip_confirm=True)

    t = workflow.add_task(
        func=say,
        params=dict(text="Hello World", out_file="out.txt"),
        uid="my_task",
        time_req=None,
        core_req=1,
        mem_req=1024,
    )

    print(("task.params", t.params))
    print(("task.input_map", t.input_map))
    print(("task.output_map", t.output_map))
    print(("task.core_req", t.core_req))
    print(("task.time_req", t.time_req))
    print(("task.drm", t.drm))
    print(("task.uid", t.uid))

    workflow.run(cmd_wrapper=py_call)

    sys.exit(0 if workflow.successful else 1)


if __name__ == "__main__":
    main()
