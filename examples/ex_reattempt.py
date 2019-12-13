"""
py_call_cmd_wrapper allows you to use pure python rather than bash scripts.  At runtime, Cosmos will create a
python script which imports the task function and calls it with the appropriate arguments.  Note you can still use
subprocess.run to call executables (like tools created by third parties).
"""
from __future__ import print_function

import os

from cosmos.api import Cosmos, py_call_cmd_wrapper


def add_one(out_file):
    if os.path.exists(out_file):
        with open(out_file) as fp:
            i = int(fp.read())
    else:
        i = 0

    with open(out_file, 'w') as fp:
        fp.write(str(i + 1))

    if i < 2:
        # fail the first 2 times
        raise


if __name__ == '__main__':
    cosmos = Cosmos('sqlite:///%s/sqlite.db' % os.path.dirname(os.path.abspath(__file__)),
                    default_drm='local')
    cosmos.initdb()
    workflow = cosmos.start('ExampleReattempt', restart=True, skip_confirm=True)

    if os.path.exists('out.txt'):
        os.unlink('out.txt')

    t = workflow.add_task(func=add_one,
                          params=dict(out_file='out.txt'),
                          uid='my_task',
                          max_attempts=3)

    workflow.make_output_dirs()
    workflow.run(cmd_wrapper=py_call_cmd_wrapper)
