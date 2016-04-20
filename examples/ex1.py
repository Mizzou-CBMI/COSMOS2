"""
Basic demonstration the structure of a Task instance
"""
import os
from cosmos.api import Cosmos

cosmos = Cosmos('sqlite:///%s/sqlite.db' % os.path.dirname(os.path.abspath(__file__)))
cosmos.initdb()

workflow = cosmos.start('Example1', 'analysis_output/ex1', restart=True, skip_confirm=True)


def say(text, out_file):
    return r"""
        echo "{text}" > {out_file}
    """.format(text=text, out_file=out_file)




t = workflow.add_task(func=say, params=dict(text='Hello World', out_file='out.txt'))

print 'Task:', t
print 'task.params', t.params
print 'task.input_files', t.input_map
print 'task.output_files', t.output_map

workflow.run()
