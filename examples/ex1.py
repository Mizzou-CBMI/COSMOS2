import os
from cosmos import Cosmos
from tools import Echo, Cat, WordCount
from cosmos.graph.draw import draw_stage_graph, draw_task_graph
import itertools as it


def main(execution):
    # Create two jobs that echo "hello" and "world" respectively
    echos = execution.add([Echo(tags=dict(word='hello'), out='{word}'),
                           Echo(tags=dict(word='world!'))])

    # Split each echo into two jobs
    cats = execution.add(Cat(tags=dict(n=n, **echo_task.tags), parents=[echo_task], out='{word}/{n}')
                         for echo_task in echos for n in [1, 2])

    # Count the words in the previous stage.  An example of a one2one relationship.
    # This is the most common stage dependency.  For each task in StageA, you create a single dependent task in StageB.
    word_counts = execution.add(WordCount(dict(chars=True, **cat_task.tags), [cat_task], '{word}/{n}')
                                for cat_task in cats)

    # Cat the contents of all word_counts into one file.  Note only one node is being created who's parents are
    # all of the WordCounts
    summarize = execution.add(Cat(tags=dict(), parents=word_counts, out=''), name='Summarize')

    # These images can also be seen on the fly in the web-interface
    draw_stage_graph(execution.stage_graph(), '/tmp/ex1_task_graph.png', format='png')
    draw_task_graph(execution.task_graph(), '/tmp//ex1_stage_graph.png', format='png')

    execution.run()


if __name__ == '__main__':
    cosmos = Cosmos('sqlite:///%s/sqlite.db' % os.path.dirname(os.path.abspath(__file__)))
    cosmos.initdb()

    execution = cosmos.start('Example1', 'out/ex1', max_attempts=2, restart=True, skip_confirm=True)
    main(execution)
