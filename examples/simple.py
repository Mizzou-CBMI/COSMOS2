from kosmos import Execution, Kosmos, rel, Recipe, Input
import tools

if __name__ == '__main__':
    kosmos_app = Kosmos('sqlite.db', default_queue='dev-short')
    kosmos_app.initdb()

    r = Recipe()

    echo = r.add_source([tools.Echo(tags={'word': 'hello'}), tools.Echo(tags={'word': 'world'})])
    cat = r.add_stage(tools.Cat, parents=[echo], rel=rel.One2many([('n', [1, 2])]))



    ex = Execution.start(kosmos_app, 'Simple', 'out/simple2', max_attempts=2, drm='ge', restart=True, skip_confirm=True)
    ex.run(r)