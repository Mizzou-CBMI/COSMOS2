import os
from cosmos.api import Cosmos, draw_stage_graph, draw_task_graph, pygraphviz_available
from tools import echo, cat, word_count


def run_ex1(execution):
    # Create two Tasks that echo "hello" and "world" respectively (these are source nodes in the dag).
    echos = [execution.add_task(echo,
                                tags=dict(word=word),
                                out_dir='{word}')
             for word in ['hello', 'world']]

    # Split each echo into two dependent Tasks (a one2many relationship).
    cats = [execution.add_task(cat,
                               tags=dict(n=n, **echo_task.tags),
                               parents=[echo_task],
                               out_dir='{word}/{n}')
            for echo_task in echos
            for n in [1, 2]]

    # Count the words in the previous stage.  An example of a one2one relationship,
    # the most common stage dependency pattern.  For each task in StageA, there is a single dependent task in StageB.
    word_counts = [execution.add_task(word_count,
                                      tags=dict(chars=True, **cat_task.tags),
                                      parents=[cat_task],
                                      out_dir='{word}/{n}')
                   for cat_task in cats]

    # Cat the contents of all word_counts into one file.  Only one node is being created who's parents are
    # all of the WordCounts (a many2one relationship).
    summarize = execution.add_task(cat,
                                   tags=dict(),
                                   parents=word_counts,
                                   out_dir='',
                                   stage_name='Summary_Analysis')

    if pygraphviz_available:
        # These images can also be seen on the fly in the web-interface
        draw_stage_graph(execution.stage_graph(), '/tmp/ex1_task_graph.png', format='png')
        draw_task_graph(execution.task_graph(), '/tmp/ex1_stage_graph.png', format='png')
    else:
        print 'Pygraphviz is not available :('

    execution.run(max_attempts=1, max_cores=10)


if __name__ == '__main__':
    cosmos = Cosmos('sqlite:///%s/sqlite.db' % os.path.dirname(os.path.abspath(__file__)))
    cosmos.initdb()

    execution = cosmos.start('Example1', 'analysis_output/ex1',restart=True, skip_confirm=True)
    run_ex1(execution)
