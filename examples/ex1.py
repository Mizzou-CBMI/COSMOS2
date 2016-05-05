"""
Basic demonstration the structure of a Task instance
"""
import subprocess as sp
import os
from cosmos.api import Cosmos, default_get_submit_args
from functools import partial

cosmos = Cosmos('sqlite:///%s/sqlite.db' % os.path.dirname(os.path.abspath(__file__)),
                get_submit_args=partial(default_get_submit_args, parallel_env='smp'),
                default_drm='local')
cosmos.initdb()

sp.check_call('mkdir -p analysis_output/ex1', shell=True)
os.chdir('analysis_output/ex1')
workflow = cosmos.start('Example1', restart=True, skip_confirm=True)


def say(text, out_file, core_req=3):
    return r"""
        echo "{text}" > {out_file}
    """.format(text=text, out_file=out_file)


t = workflow.add_task(func=say, params=dict(text='Hello World', out_file='out.txt', core_req=5), uid='my_task')

print 'task.params', t.params
print 'task.input_map', t.input_map
print 'task.output_map', t.output_map
print 'task.log_dir', t.log_dir
print 'task.core_req', t.core_req
print 'task.drm', t.drm
print 'task.uid', t.uid

workflow.run()
