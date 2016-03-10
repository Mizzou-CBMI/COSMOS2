import subprocess as sp
import contextlib
import os


@contextlib.contextmanager
def cd(path):
    """A context manager which changes the working directory to the given
    path, and then changes it back to its previous value on exit.

    """
    prev_cwd = os.getcwd()
    os.chdir(path)
    yield
    os.chdir(prev_cwd)


def run(cmd):
    return sp.check_call(cmd, shell=True)


def test_ex1():
    with cd(os.path.join(os.path.dirname(__file__), '../examples')):
        run('python ex1.py')

def test_ex2():
    with cd(os.path.join(os.path.dirname(__file__), '../examples')):
        run('python ex2.py')