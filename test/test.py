def run():
    import os
    from tools import ECHO, CAT
    from kosmos import rel, Recipe, Execution
    from kosmos.db import session
    opj = os.path.join

    r = Recipe()
    echo = r.add_source([ECHO(tags={'word': 'hello'}), ECHO(tags={'word': 'world'}), ECHO(tags={'word': 'world2'})])
    cat = r.add_stage(CAT, parents=[echo], rel=rel.One2many([('n', [1, 2])]))


    ex = Execution.start(session, name='test', output_dir='/locus/home/egafni/tmp')
    ex.run(r,
           lambda x: opj(x.execution.output_dir),
           lambda t: opj(t.output_dir, 'log', t.stage.name, t.tags['word']))


if __name__ == '__main__':
    import ipdb;
    with ipdb.launch_ipdb_on_exception():

        run()

