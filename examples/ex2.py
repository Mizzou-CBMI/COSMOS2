import os
import subprocess as sp
from cosmos.api import Cosmos, Dependency, draw_stage_graph, draw_task_graph, \
    pygraphviz_available, default_get_submit_args
from functools import partial
from tools import echo, cat, word_count


def recipe(workflow):
    # Create two Tasks that echo "hello" and "world" respectively (source nodes of the dag).
    echo_tasks = [workflow.add_task(func=echo,
                                    params=dict(word=word, out_txt='%s.txt' % word),
                                    uid=word)
                  for word in ['hello', 'world']]

    # Split each echo into two dependent Tasks (a one2many relationship).
    word_count_tasks = []
    for echo_task in echo_tasks:
        word = echo_task.params['word']
        for n in [1, 2]:
            cat_task = workflow.add_task(
                func=cat,
                params=dict(in_txts=[echo_task.params['out_txt']],
                            out_txt='%s/%s/cat.txt' % (word, n)),
                parents=[echo_task],
                uid='%s_%s' % (word, n))

            # Count the words in the previous stage.  An example of a simple one2one relationship
            # For each task in StageA, there is a single dependent task in StageB.
            word_count_task = workflow.add_task(
                func=word_count,
                # Dependency instances allow you to specify an input and parent simultaneously
                params=dict(in_txts=[Dependency(cat_task, 'out_txt')],
                            out_txt='%s/%s/wc.txt' % (word, n),
                            chars=True),
                # parents=[cat_task], <-- not necessary!
                uid='%s_%s' % (word, n), )
            word_count_tasks.append(word_count_task)

    # Cat the contents of all word_counts into one file.  Only one node is being created who's
    # parents are all of the WordCounts (a many2one relationship, aka a reduce operation).
    summarize_task = workflow.add_task(
        func=cat,
        params=dict(in_txts=[Dependency(t, 'out_txt') for t in word_count_tasks],
                    out_txt='summary.txt'),
        parents=word_count_tasks,
        stage_name='Summary_Analysis',
        uid='')  # It's the only Task in this Stage, so doesn't need a specific uid


if __name__ == '__main__':
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument('-drm', default='local', help='', choices=('local', 'drmaa:ge', 'ge'))
    p.add_argument('-q', '--queue', help='Submit to this queue of the DRM supports it')

    args = p.parse_args()

    cosmos = Cosmos('sqlite:///%s/sqlite.db' % os.path.dirname(os.path.abspath(__file__)),
                    # example of how to change arguments if you're NOT using default_drm='local'
                    get_submit_args=partial(default_get_submit_args, parallel_env='smp'),
                    default_drm=args.drm,
                    default_queue=args.queue)
    cosmos.initdb()

    sp.check_call('mkdir -p analysis_output/ex2', shell=True)
    os.chdir('analysis_output/ex2')

    workflow = cosmos.start('Example2', restart=True, skip_confirm=True)

    recipe(workflow)

    workflow.make_output_dirs()
    workflow.run(max_attempts=1, max_cores=10)

    if pygraphviz_available:
        # These images can also be seen on the fly in the web-interface
        draw_stage_graph(workflow.stage_graph(), '/tmp/ex1_task_graph.png', format='png')
        draw_task_graph(workflow.task_graph(), '/tmp/ex1_stage_graph.png', format='png')
    else:
        print 'Pygraphviz is not available :('
