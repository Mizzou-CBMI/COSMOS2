from cosmos.api import Cosmos, py_call


def noop():
    pass


def test_zero_tasks(cleandir):
    cosmos = Cosmos()
    cosmos.initdb()
    workflow = cosmos.start("workflow", skip_confirm=True)
    workflow.run(set_successful=False)
    workflow.run(cmd_wrapper=py_call)


if __name__ == "__main__":
    test_zero_tasks()
