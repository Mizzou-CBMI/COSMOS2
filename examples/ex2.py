import os
from cosmos import Cosmos, one2one
from tools import Echo, Cat, WordCount
from cosmos.util.helpers import mkdir


def main(execution):
    """
    This is equivalent to example1, but using a helper function :meth:`one2one` for simple relationship patterns between
    stages. The one2one, many2one, etc functions are simple and it is highly recommended that you read through their
    code, and make your own functions for similar patterns.
    """

    # Create two jobs that echo "hello" and "world" respectively
    ech = execution.add([Echo(tags=dict(word='hello'), out='{word}'),
                         Echo(tags=dict(word='world'))])

    # Split each echo into two jobs
    cat = execution.add(Cat(tags=dict(n=n, **echo_task.tags), parents=[echo_task], out='{word}/{n}')
                        for echo_task in ech for n in [1, 2])

    # Count the words in the previous stage
    wdc = execution.add(one2one(WordCount, cat))
    # equivalent to:
    # wdc = execution.add(WordCount(cat_task.tags, [cat_task], '{word}/{n}')
    #                    for cat_task in cat)

    execution.run()


if __name__ == '__main__':
    cosmos = Cosmos('sqlite:///%s/sqlite.db' % os.path.dirname(os.path.abspath(__file__)))
    cosmos.initdb()
    mkdir('out')

    execution = cosmos.start('Example2', 'out/ex2', max_attempts=2, restart=True, skip_confirm=True)
    main(execution)
