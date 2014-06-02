"""
If a Task fails, all of it's descendants will not be executed, however, the rest of the DAG will be.
"""
import subprocess as sp

from kosmos import Execution, KosmosApp, rel, Recipe, Input
import tools


r = Recipe()
inp = r.add_source([Input('blah', '/tmp', dict(test='tag'))])
fail = r.add_stage(tools.Fail, inp)

sleep = r.add_stage(tools.sleep, tags=dict(time=5))
echo = r.add_source([tools.Echo(dict(word='hello')), tools.Echo(tags=dict(word='world'))])

#cat will not run, but echo will
cat = r.add_stage(tools.Cat, parents=[echo, fail], rel=rel.One2many([('n', [1, 2])]))

kosmos_app = KosmosApp('sqlite:///simple.db', default_drm='local')
kosmos_app.initdb()

sp.check_call(['mkdir', '-p', 'out'])
ex = Execution.start(kosmos_app=kosmos_app, output_dir='out/failed_task', name='Failed_Task', restart=True, max_attempts=1)
ex.run(r)