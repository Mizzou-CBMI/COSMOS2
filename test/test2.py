import os
from tools import ECHO, CAT
from kosmos import rel, Recipe
opj = os.path.join

def run(ex, **kwargs):
    r = Recipe()
    echo = r.add_source([ECHO(tags={'word': 'hello'}), ECHO(tags={'word': 'world'}), ECHO(tags={'word': 'world2'})])

    ex.run(r)
    cat = r.add_stage(CAT, parents=[echo], rel=rel.One2many([('n', [1, 2])]))

    ex.run(r)


if __name__ == '__main__':
    from kosmos import default_argparser
    import ipdb
    with ipdb.launch_ipdb_on_exception():
        ex, kwargs = default_argparser()
        run(ex, **kwargs)