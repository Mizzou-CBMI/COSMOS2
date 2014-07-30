from cosmos import Execution, Cosmos, rel, Recipe
import itertools as it

import tools
from cosmos.models.Tool import chain

if __name__ == '__main__':
    import ipdb
    with ipdb.launch_ipdb_on_exception():
        cosmos_app = Cosmos('sqlite.db', default_queue='dev-short', default_drm='local')
        cosmos_app.initdb()

        recipe = Recipe()

        echo = recipe.add_source([tools.Echo(tags={'word': 'hello'}), tools.Echo(tags={'word': 'world'})])
        #cat = r.add_stage(tools.Cat, parents=[echo], rel=rel.One2many([('n', [1, 2])]))
        cat = recipe.add_stage(chain(tools.Cat, tools.WordCount), echo, rel.One2many([('n', [1, 2])]))

        ex = Execution.start(cosmos_app, 'Simple', 'out/simple2', max_attempts=2, restart=True, skip_confirm=True)
        ex.run(recipe)
