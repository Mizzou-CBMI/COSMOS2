from kosmos import Execution, Kosmos, rel, Recipe
import itertools as it

import tools
from kosmos.models.Tool import collapse_tools

if __name__ == '__main__':
    import ipdb
    with ipdb.launch_ipdb_on_exception():
        kosmos_app = Kosmos('sqlite.db', default_queue='dev-short')
        kosmos_app.initdb()

        r = Recipe()

        echo = r.add_source([tools.Echo(tags={'word': 'hello'}), tools.Echo(tags={'word': 'world'})])
        #cat = r.add_stage(tools.Cat, parents=[echo], rel=rel.One2many([('n', [1, 2])]))
        cat = r.add_stage(collapse_tools(tools.Cat, tools.WordCount), echo, rel.One2many([('n', [1, 2])]))

        ex = Execution.start(kosmos_app, 'Simple', 'out/simple2', max_attempts=2, drm='local', restart=True, skip_confirm=True)
        ex.run(r)
