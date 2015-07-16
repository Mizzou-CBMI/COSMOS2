import os
from cosmos import Cosmos
from tools import Echo, Cat, WordCount
from cosmos.graph.draw import draw_stage_graph, draw_task_graph, pygraphviz_available
from cosmos.util.helpers import mkdir


import itertools as it

def run_ex1(execution):
    # Create two jobs that echo "hello" and "world" respectively (source nodes in the graph).
    echos = execution.add([Echo(tags=dict(word='hello'), out='{word}'),
                           Echo(tags=dict(word='world!'))])

    # Split each echo into two jobs (a one2many relationship).
    cats = execution.add(Cat(tags=dict(n=n, **echo_task.tags), parents=[echo_task], out='{word}/{n}')
                         for echo_task in echos for n in [1, 2])

    # Count the words in the previous stage.  An example of a one2one relationship,
    # the most common stage dependency pattern.  For each task in StageA, you create a single dependent task in StageB.
    word_counts = execution.add(WordCount(dict(chars=True, **cat_task.tags), [cat_task], '{word}/{n}')
                                for cat_task in cats)

    # Cat the contents of all word_counts into one file.  Note only one node is being created who's parents are
    # all of the WordCounts (a many2one relationship).
    summarize = execution.add(Cat(tags=dict(), parents=word_counts, out=''), name='Summarize')

    if pygraphviz_available:
        # These images can also be seen on the fly in the web-interface
        draw_stage_graph(execution.stage_graph(), '/tmp/ex1_task_graph.png', format='png')
        draw_task_graph(execution.task_graph(), '/tmp//ex1_stage_graph.png', format='png')
    else:
        print 'Pygraphviz is not available :('

    execution.run()


if __name__ == '__main__':
    cosmos = Cosmos('sqlite:///%s/sqlite.db' % os.path.dirname(os.path.abspath(__file__)))
    cosmos.initdb()
    mkdir('out')

    execution = cosmos.start('Example1', 'out/ex1', max_attempts=2, restart=True, skip_confirm=True)
    run_ex1(execution)
