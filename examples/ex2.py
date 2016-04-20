import os
import subprocess as sp
from cosmos.api import Cosmos, draw_stage_graph, draw_task_graph, pygraphviz_available
from tools import echo, cat, word_count


def run_ex1(workflow):
    # Create two Tasks that echo "hello" and "world" respectively (these are source nodes in the dag).
    echo_tasks = [workflow.add_task(func=echo,
                                    params=dict(word=word, out_txt='%s.txt' % word))
                  for word in ['hello', 'world']]

    # Split each echo into two dependent Tasks (a one2many relationship).
    word_count_tasks = []
    for echo_task in echo_tasks:
        word = echo_task.params['word']
        for n in [1, 2]:
            cat_task = workflow.add_task(cat,
                                         params=dict(in_txts=[echo_task.params['out_txt']],
                                                     out_txt='%s/%s/cat.txt' % (word, n)),
                                         parents=[echo_task])

            # Count the words in the previous stage.  An example of a one2one relationship,
            # the most common stage dependency pattern.  For each task in StageA, there is a single dependent task in StageB.
            word_count_task = workflow.add_task(func=word_count,
                                                params=dict(in_txts=[cat_task.params['out_txt']],
                                                            out_txt='%s/%s/wc.txt' % (word, n),
                                                            chars=True),
                                                parents=[cat_task])
            word_count_tasks.append(word_count_task)

    # Cat the contents of all word_counts into one file.  Only one node is being created who's parents are
    # all of the WordCounts (a many2one relationship).
    summarize_task = workflow.add_task(func=cat,
                                       params=dict(in_txts=[t.params['out_txt'] for t in word_count_tasks],
                                                   out_txt='summary.txt'),
                                       parents=word_count_tasks,
                                       stage_name='Summary_Analysis')

    if pygraphviz_available:
        # These images can also be seen on the fly in the web-interface
        draw_stage_graph(workflow.stage_graph(), '/tmp/ex1_task_graph.png', format='png')
        draw_task_graph(workflow.task_graph(), '/tmp/ex1_stage_graph.png', format='png')
    else:
        print 'Pygraphviz is not available :('

    workflow.run(max_attempts=1, max_cores=10)


if __name__ == '__main__':
    cosmos = Cosmos('sqlite:///%s/sqlite.db' % os.path.dirname(os.path.abspath(__file__)))
    cosmos.initdb()

    sp.check_call('mkdir -p analysis_output/ex1', shell=True)
    workflow = cosmos.start('Example1', 'analysis_output/ex1', restart=True, skip_confirm=True)
    run_ex1(workflow)
