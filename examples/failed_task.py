"""
If a Task fails, all of it's descendants will not be executed, however, the rest of the DAG will be.
"""
import subprocess as sp
import os

from kosmos import Execution, KosmosApp, rel, Recipe, Input
import tools


r = Recipe()
inpt = r.add_source([Input('tmp_dir', 'dir', '/tmp', dict(test='tag'))])

echo = r.add_source([tools.Echo(dict(word='hello')), tools.Echo(tags=dict(word='world'))])

fail = r.add_stage(tools.Fail, inpt)
sleep = r.add_stage(tools.Sleep, [inpt], add_tags=dict(time=5))

#cat will not run, but echo will
cat = r.add_stage(tools.Cat, parents=[echo, fail], rel=rel.Many2many([], [('n', [1, 2])]))

kosmos_app = KosmosApp('sqlite:///%s/simple.db' % os.getcwd())
kosmos_app.initdb()
sp.check_call(['mkdir', '-p', 'out'])
ex = Execution.start(kosmos_app=kosmos_app, output_dir='out/failed_task', drm='local', name='Failed_Task', restart=True, max_attempts=1, skip_confirm=True)
ex.run(r)