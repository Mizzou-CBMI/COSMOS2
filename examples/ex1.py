import os
from cosmos import Cosmos
from tools import Echo, Cat, WordCount


def ex1_main(execution):
    # Create two jobs that echo "hello" and "world" respectively
    ech = execution.add([Echo(tags=dict(word='hello'), out='{word}'), Echo(tags=dict(word='world'))])

    # Split each echo into two jobs
    cat = execution.add([Cat(tags=dict(n=n, **echo_task.tags), parents=[echo_task], out='{word}/{n}')
                         for echo_task in ech for n in [1, 2]])

    # Count the words in the previous stage
    wdc = execution.add([WordCount(cat_task.tags, [cat_task], '{word}/{n}')
                         for cat_task in cat])

    execution.run()


if __name__ == '__main__':
    cosmos = Cosmos('sqlite:///%s/sqlite.db' % os.path.dirname(os.path.abspath(__file__)))
    cosmos.initdb()

    execution = cosmos.start('Example1', 'out/ex1', max_attempts=2, restart=True, skip_confirm=True)
    ex1_main(execution)
