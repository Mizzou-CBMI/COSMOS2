import os
from tools import ECHO, CAT
from kosmos import rel, TaskGraph, Recipe, Execution
from kosmos.db import session
opj = os.path.join


def run():
    r = Recipe()
    echo = r.add_source([ECHO(tags={'word': 'hello'}), ECHO(tags={'word': 'world'})])
    cat = r.add_stage(CAT, parents=[echo], rel=rel.One2many([('n', [1, 2])]))

    tg = TaskGraph(r)
    e = Execution()
    e = e.run(tg,  lambda x: '/tmp', lambda t: os.path.join(t.output_dir, 'log', t.stage.name, t.tags['word']), session=session)
    session.add(e)
    session.commit()


if __name__ == '__main__':
    run()