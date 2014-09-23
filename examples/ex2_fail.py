"""
If a Task fails, all of it's descendants will not be executed, however, the rest of the DAG will be.
"""

from cosmos import Input

from cosmos import Execution, Cosmos, rel, Recipe
import tools


def ex2_main(execution):
    recipe = Recipe()

    inpt = recipe.add_source([Input('/tmp', 'tmp_dir', 'dir', dict(test='tag'))])
    echo = recipe.add_source([tools.Echo(dict(word='hello')), tools.Echo(tags=dict(word='world'))])
    fail = recipe.add_stage(tools.Fail, inpt)
    sleep = recipe.add_stage(tools.Sleep, [inpt], tag=dict(time=5))
    cat = recipe.add_stage(tools.Cat, parents=[echo, fail], rel=rel.Many2many([], [('n', [1, 2])]))

    execution.run(recipe)


if __name__ == '__main__':
    cosmos_app = Cosmos('sqlite:///sqlite.db')
    cosmos_app.initdb()

    ex = Execution.start(cosmos_app, 'Failed_Task', 'out/failed_task', max_attempts=2, restart=True, skip_confirm=True)
    ex2_main(ex)