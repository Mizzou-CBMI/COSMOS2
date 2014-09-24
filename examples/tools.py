from cosmos import Tool, abstract_input_taskfile as itf, abstract_output_taskfile as otf
from main import settings as s

class Sleep(Tool):
    def cmd(self, i, o, time=10):
        return 'sleep {time}'


class Echo(Tool):
    outputs = [otf('echo', 'txt')]

    def cmd(self, _, outputs, word):
        return '{s[echo_path]} {word} > {outputs[0]}'.format(s=s, **locals())


class Cat(Tool):
    inputs = [itf(format='txt', n='>=1')]
    outputs = [otf('cat', 'txt', 'cat_out.txt', )]

    def cmd(self, inputs, (out_txt,)):
        return 'cat {input} > {out_txt}'.format(
            input=' '.join(map(str, inputs)),
            **locals()
        )


class Paste(Tool):
    inputs = [itf(format='txt')]
    outputs = [otf('paste', 'txt', 'paste.txt')]

    def cmd(self, inputs, (out_txt,)):
        return 'paste {input} > {out_txt}'.format(
            input=' '.join(map(str, inputs)),
            **locals()
        )


class WordCount(Tool):
    inputs = [itf(format='txt')]
    outputs = [otf('wc', 'txt')]

    def cmd(self, inputs, (out_txt,)):
        return 'wc {input} > {out_txt}'.format(
            input=' '.join(map(str, inputs)),
            **locals()
        )


class Fail(Tool):
    def cmd(self, i, o):
        return '__fail__'


class MD5Sum(Tool):
    inputs = [itf(format='*', n=1)]
    outputs = [otf(name='md5', format='md5')]

    def cmd(self, in_file, out_md5):
        out_md5.basename = in_file.basename + '.md5'
        return 'md5sum {in_file}'.format(**locals())