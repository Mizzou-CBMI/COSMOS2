from tools import ECHO, CAT
from kosmos import run, one2many, TaskGraph
import os
opj = os.path.join

import ipdb
with ipdb.launch_ipdb_on_exception():
    g = TaskGraph()
    echo = g.source([ECHO(tags={'word': 'hello'}), ECHO(tags={'word': 'world'})])
    cat = g.stage(CAT, parents=[echo], rel=one2many([('n', [1, 2])]))

    run(g, lambda x: '/tmp', lambda t: os.path.join(t.output_dir, 'log', t.stage.name, t.tags['word']))

    # g.as_image('stage', 'graph1.svg')
    # g.as_image('task', 'graph2.svg')