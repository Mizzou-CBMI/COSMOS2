import os

from tools import ECHO, CAT
from kosmos.ToolGraph import ToolGraph, one2many

opj = os.path.join


g = ToolGraph()
echo = g.source([ECHO(tags={'word': 'hello'}), ECHO(tags={'word': 'world'})])
cat  = g.stage(CAT, parents=[echo], rel=one2many([('n', [1, 2])]))
g.resolve()
g.as_image('stage', 'graph1.svg')
g.as_image('tool', 'graph2.svg')
