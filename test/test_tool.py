from cosmos import Tool, abstract_input_taskfile as aif, abstract_output_taskfile as aof
from main import settings as s


class Sleep(Tool):
    def cmd(self, time=10):
        return 'sleep %s' % time


class Echo(Tool):
    def cmd(self, word, out_txt=aof('echo.txt')):
        return '{s[echo_path]} {word} > {out_txt}'.format(s=s, **locals())


class Cat(Tool):
    def cmd(self, inputs=aif(format='txt', n='>=1'), out_txt=aof('cat.txt')):
        return 'cat {input_str} > {out_txt}'.format(
            input_str=' '.join(map(str, inputs)),
            **locals()
        )

class Paste(Tool):
    def cmd(self, input_txts=aif(format='txt', n='>=1'), out_txt=aof('paste.txt')):
        return 'paste {input} > {out_txt}'.format(
            input=' '.join(map(str, (input_txts,))),
            **locals()
        )


class WordCount(Tool):
    def cmd(self, chars=False, input_txts=aif(format='txt', n='>=1'), out_txt=aof('wc.txt')):
        c = ' -c' if chars else ''
        return 'wc{c} {input} > {out_txt}'.format(
            input=' '.join(map(str, input_txts)),
            **locals()
        )


class Fail(Tool):
    def cmd(self):
        return '__fail__'


class MD5Sum(Tool):
    def cmd(self, in_file=aif(format='*', n=1), out_md5=aof('checksum.md5')):
        out_md5.basename = in_file.basename + '.md5'
        return 'md5sum {in_file}'.format(**locals())


import os
from cosmos import Cosmos
from cosmos.graph.draw import draw_stage_graph, draw_task_graph, pygraphviz_available
from cosmos.util.helpers import mkdir


import itertools as it

def run_ex1(execution):
    # Create two jobs that echo "hello" and "world" respectively (source nodes in the graph).
    echos = execution.add([Echo(tags=dict(word='hello'), out='{word}'),
                           Echo(tags=dict(word='world'))])

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