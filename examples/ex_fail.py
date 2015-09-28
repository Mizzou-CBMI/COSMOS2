"""
If a Task fails, all of it's descendants will not be executed, however, the rest of the DAG will be.
"""

from cosmos.api import Cosmos, Input
import tools
from cosmos.util.helpers import mkdir


def run_ex2(execution):
    # TODO update this
    pass
    # # These tasks have no dependencies
    # inpts = execution.add([Input('/tmp', 'tmp_dir', 'dir', dict(test='tag'))])
    # echos = execution.add([tools.Echo(dict(word='hello')), tools.Echo(tags=dict(word='world'))])
    #
    # # This task always fails
    # fails = execution.add(tools.Fail, inpts)
    #
    # # Not dependent on the task that failed, will be executed
    # sleeps = execution.add(tools.Sleep(dict(time=5), [inp]) for inp in inpts)
    #
    # # This will not run, because it depends on a task that failed
    # cats = execution.add(tools.Cat(parents=[echos[0], fails[0]]))
    #
    # execution.run()


if __name__ == '__main__':
    cosmos = Cosmos('sqlite:///sqlite.db')
    cosmos.initdb()
    mkdir('out_dir')

    ex = cosmos.start('Failed_Task', 'out_dir/failed_task', max_attempts=2, restart=True, skip_confirm=True)
    run_ex2(ex)