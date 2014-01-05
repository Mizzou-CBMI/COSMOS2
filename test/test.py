def run():
    import os
    from tools import ECHO, CAT
    from kosmos import rel, TaskGraph, Recipe, Execution
    from kosmos.db import session
    opj = os.path.join

    r = Recipe()
    echo = r.add_source([ECHO(tags={'word': 'hello'}), ECHO(tags={'word': 'world'})])
    cat = r.add_stage(CAT, parents=[echo], rel=rel.One2many([('n', [1, 2])]))

    tg = TaskGraph(r)
    e = Execution(output_dir='/home/egafni/tmp')
    session.add(e)
    session.commit()
    e = e.run(tg,
              lambda x: '/home/egafni/tmp',
              lambda t: os.path.join(t.output_dir, 'log', t.stage.name, t.tags['word']))


if __name__ == '__main__':
    import ipdb;
    with ipdb.launch_ipdb_on_exception():
        run()