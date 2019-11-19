"""
py_call() allows you to use python functions as Tasks functions.  It will autogenerate a python script
which imports the functions, and calls it with the params and use that for the command_script.
"""

import os

from cosmos.api import Cosmos, py_call


def say(text, out_file):
    with open(out_file, 'w') as fp:
        print(text, file=fp)


if __name__ == '__main__':
    cosmos = Cosmos('sqlite:///%s/sqlite.db' % os.path.dirname(os.path.abspath(__file__)),
                    default_drm='local')
    cosmos.initdb()
    workflow = cosmos.start('Example3', restart=True, skip_confirm=True)

    t = workflow.add_task(func=py_call(say),
                          params=dict(text='Hello World', out_file='out.txt'),
                          uid='my_task')

    workflow.make_output_dirs()
    workflow.run()
