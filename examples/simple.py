from cosmos import Execution, Cosmos, rel, Recipe
import tools

if __name__ == '__main__':
    cosmos_app = Cosmos('sqlite.db', default_queue='dev-short', default_drm='local')
    cosmos_app.initdb()

    recipe = Recipe()

    echo = recipe.add_source([tools.Echo(tags={'word': 'hello'}), tools.Echo(tags={'word': 'world'})])
    #cat = r.add_stage(tools.Cat, parents=[echo], rel=rel.One2many([('n', [1, 2])]))
    cat = recipe.add_stage(tools.Cat, echo, rel.One2many([('n', [1, 2])]))
    wc = recipe.add_stage(tools.WordCount, cat)

    ex = Execution.start(cosmos_app, 'Simple', 'out/simple2', max_attempts=2, restart=True, skip_confirm=True)
    ex.run(recipe)
