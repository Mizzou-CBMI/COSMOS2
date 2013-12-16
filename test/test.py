import os

from tasks import ECHO, CAT
from kosmos.TaskGraph import TaskGraph, one2many
from kosmos.Runner import Runner

opj = os.path.join

g = TaskGraph()
echo = g.source([ECHO(tags={'word': 'hello'}), ECHO(tags={'word': 'world'})])
cat = g.stage(CAT, parents=[echo], rel=one2many([('n', [1, 2])]))
r = Runner(g, lambda x: '/tmp', lambda t: os.path.join(t.output_dir, 'log', t.stage.name, t.tags['word'])).run()
g.as_image('stage', 'graph1.svg')
g.as_image('task', 'graph2.svg')