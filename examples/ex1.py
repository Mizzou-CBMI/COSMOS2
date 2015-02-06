from cosmos import Cosmos
from tools import Echo, Cat, WordCount


def ex1_main(execution):

    # Create two jobs that echo "hello" and "world" respectively
    ech = execution.add_stage([Echo(tags=dict(word='hello'), out='{word}'), Echo(tags=dict(word='world'))])

    # Split each echo into two jobs
    cat = execution.add_stage([Cat(tags=dict(n=n, **echo_task.tags), parents=[echo_task], out='{word}/{n}')
                               for echo_task in ech.tasks for n in [1, 2]])

    # Count the words in the previous stage
    wdc = execution.add_stage([WordCount(cat_task.tags, [cat_task], '{word}/{n}')
                               for cat_task in cat.tasks])

    execution.run()


if __name__ == '__main__':
    cosmos = Cosmos('sqlite:///sqlite.db')
    cosmos.initdb()

    execution = cosmos.start('Example1', 'out/ex1', max_attempts=2, restart=True, skip_confirm=True)
    ex1_main(execution)
