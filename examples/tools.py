from cosmos import Tool, abstract_input_taskfile as itf, abstract_output_taskfile as otf
from ..main import settings as s

class Sleep(Tool):
    def cmd(self, i, o, time=10):
        return 'sleep {time}'


class Echo(Tool):
    outputs = [otf('echo', 'txt')]

    def cmd(self, i, o, word):
        return '{s[echo_path]} {word} > {o[echo]}'.format(s=s, **locals())


class Cat(Tool):
    inputs = [itf(format='txt', n='>=1')]
    outputs = [otf('cat', 'txt', 'cat_out.txt', )]

    def cmd(self, i, o):
        return 'cat {input} > {o[cat]}'.format(
            input=' '.join(map(str, i.format['txt'])),
            **locals()
        )


class Paste(Tool):
    inputs = [itf(format='txt')]
    outputs = [otf('paste', 'txt', 'paste.txt')]

    def cmd(self, i, o):
        return 'paste {input} > {o[paste]}'.format(
            input=' '.join(map(str, i.format['txt'])),
            **locals()
        )


class WordCount(Tool):
    inputs = [itf(format='txt')]
    outputs = [otf('wc', 'txt')]

    def cmd(self, i, o):
        return 'wc {input} > {o[wc]}'.format(
            input=' '.join(map(str, i.format['txt'])),
            **locals()
        )


class Fail(Tool):
    def cmd(self, i, o):
        return '__fail__'


class MD5Sum(Tool):
    inputs = [itf(format='*')]
    outputs = [otf(name='md5', format='md5', basename="{i.format[*].basename}.md5")]

    def cmd(self, i, o):
        return 'md5sum {inp}'.format(inp=" ".join(map(str, i.values())))
