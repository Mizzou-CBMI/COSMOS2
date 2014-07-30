"""
If a Task fails, all of it's descendants will not be executed, however, the rest of the DAG will be.
"""
import os

from cosmos import Execution, Cosmos, rel, Recipe, Input
import tools

if __name__ == '__main__':
    r = Recipe()
    inpt = r.add_source([Input('tmp_dir', 'dir', '/tmp', dict(test='tag'))])

    echo = r.add_source([tools.Echo(dict(word='hello')), tools.Echo(tags=dict(word='world'))])

    fail = r.add_stage(tools.Fail, inpt)
    sleep = r.add_stage(tools.Sleep, [inpt], tag=dict(time=5))

    cat = r.add_stage(tools.Cat, parents=[echo, fail], rel=rel.Many2many([], [('n', [1, 2])]))

    cosmos_app = Cosmos('sqlite.db')
    cosmos_app.initdb()
    ex = Execution.start(cosmos_app=cosmos_app, output_dir='out/Failed_Task', drm='local', name='Failed_Task', restart=True, max_attempts=1, skip_confirm=True)
    ex.run(r)