import os
from tools import ECHO, CAT
from kosmos import rel, Recipe, Input
opj = os.path.join

def run(ex, **kwargs):
    r = Recipe()
    inp = r.add_source([Input('blah', '/tmp', {'test':'tag'})])
    echo = r.add_source([ECHO(tags={'word': 'hello'}), ECHO(tags={'word': 'world'}), ECHO(tags={'word': 'world2'})])
    cat = r.add_stage(CAT, parents=[echo], rel=rel.One2many([('n', [1, 2])]))

    ex.run(r, lambda x: x.execution.output_dir)


if __name__ == '__main__':
    from kosmos import default_argparser
    import ipdb
    with ipdb.launch_ipdb_on_exception():
        ex, kwargs = default_argparser()
        run(ex, **kwargs)