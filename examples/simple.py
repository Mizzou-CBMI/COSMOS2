from kosmos import Execution, KosmosApp, rel, Recipe, Input
import tools

if __name__ == '__main__':
    r = Recipe()
    inp = r.add_source([Input('blah', '/tmp', {'test': 'tag'})])
    fail = r.add_stage(tools.Fail, inp)
    echo = r.add_source([tools.Echo(tags={'word': 'hello'}), tools.Echo(tags={'word': 'world'})])
    cat = r.add_stage(tools.Cat, parents=[echo], rel=rel.One2many([('n', [1, 2])]))

    kosmos_app = KosmosApp('sqlite:///simple.db', default_drm='local')
    kosmos_app.initdb()
    ex = Execution.start(kosmos_app=kosmos_app, output_dir='out/test', name='test', restart=True, max_attempts=2)
    ex.run(r, lambda x: x.execution.output_dir)