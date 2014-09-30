from cosmos import Tool, abstract_input_taskfile as inp, abstract_output_taskfile as out
from main import settings as s

class Sleep(Tool):
    def cmd(self, i, o, time=10):
        return 'sleep {time}'


class Echo(Tool):
    outputs = [out('echo', 'txt')]

    def cmd(self, _, outputs, word):
        return '{s[echo_path]} {word} > {outputs[0]}'.format(s=s, **locals())


class Cat(Tool):
    inputs = [inp(format='txt', n='>=1')]
    outputs = [out('cat', 'txt', 'cat_out.txt', )]

    def cmd(self, inputs, outputs):
        input_txts = inputs[0]  # it's easier to just unpack in the method signature, see examples below.
        out_txt = outputs[0]  # it's easier to just unpack in the method signature, see examples below.
        return 'cat {input} > {out_txt}'.format(
            input=' '.join(map(str, (input_txts,))),
            **locals()
        )


class Paste(Tool):
    inputs = [inp(format='txt')]
    outputs = [out('paste', 'txt', 'paste.txt')]

    def cmd(self, (input_txts,), (out_txt,)):
        return 'paste {input} > {out_txt}'.format(
            input=' '.join(map(str, (input_txts,))),
            **locals()
        )


class WordCount(Tool):
    inputs = [inp(format='txt')]
    outputs = [out('wc', 'txt')]

    def cmd(self, (input_txts,), (out_txt,)):
        return 'wc {input} > {out_txt}'.format(
            input=' '.join(map(str, (input_txts,))),
            **locals()
        )


class Fail(Tool):
    def cmd(self, inputs, outputs):
        return '__fail__'


class MD5Sum(Tool):
    inputs = [inp(format='*', n=1)]
    outputs = [out(name='md5', format='md5')]

    def cmd(self, (in_file,), _, out_md5):
        out_md5.basename = in_file.basename + '.md5'
        return 'md5sum {in_file}'.format(**locals()) 
