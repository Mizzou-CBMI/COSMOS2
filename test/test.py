import os
from tools import Echo, Cat
from kosmos import rel, Recipe, Input

opj = os.path.join


def run(ex, **kwargs):
    r = Recipe()
    inp = r.add_source([Input('blah', '/tmp', {'test': 'tag'})])
    echo = r.add_source([Echo(tags={'word': 'hello'}), Echo(tags={'word': 'world'}), Echo(tags={'word': 'world2'})])
    cat = r.add_stage(Cat, parents=[echo], rel=rel.One2many([('n', [1, 2])]))

    ex.run(r, lambda x: x.execution.output_dir)


if __name__ == '__main__':
    from kosmos import default_argparser, db
    import ipdb

    with ipdb.launch_ipdb_on_exception():
        # get the directory this file is stored in
        test_dir = os.path.dirname(os.path.realpath(__file__))
        root_output_dir = os.path.join(test_dir, 'out')
        session = db.initdb('sqlite:///test.db', echo=False)
        ex, kwargs = default_argparser(session, root_output_dir)
        run(ex, **kwargs)