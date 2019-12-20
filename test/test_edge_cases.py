import shutil
import tempfile

from cosmos.api import Cosmos, cd, py_call


def noop():
    pass


def test_zero_tasks():
    cosmos = Cosmos()
    cosmos.initdb()
    temp_dir = tempfile.mkdtemp()
    with cd(temp_dir):
        workflow = cosmos.start('workflow', skip_confirm=True)
        workflow.run(set_successful=False)
        workflow.run(cmd_wrapper=py_call)

    shutil.rmtree(temp_dir)


if __name__ == '__main__':
    test_zero_tasks()
