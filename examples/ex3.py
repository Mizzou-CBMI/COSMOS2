"""
Basic demonstration the structure of a Task instance
"""
import os
from cosmos.api import Cosmos

cosmos = Cosmos('sqlite:///%s/sqlite.db' % os.path.dirname(os.path.abspath(__file__)))
cosmos.initdb()

execution = cosmos.start('Example1', 'analysis_output/ex3', restart=True, skip_confirm=True)


def cmd(in_files, out_file):
    return r"""
        echo "{in_files}" > {out_file}
    """.format(**locals())


t = execution.add_task(cmd, tags=dict(in_files=[('a', 'b', 'in_file')], out_file='out.txt'))

print 'Task:', t
print 'task.tags', t.tags
print 'task.input_files', t.input_files
print 'task.output_files', t.output_files

#execution.run()

