from kosmos import Tool, input_taskfile as itf, output_taskfile as otf


class Sleep(Tool):
    def cmd(self, i, o, s, time=10):
        return 'sleep {time}'


class Echo(Tool):
    outputs = [itf('echo', 'txt')]

    def cmd(self, i, o, s, word):
        return 'echo {word} > {o[txt]}'


class Cat(Tool):
    inputs = [itf(format='txt')]
    outputs = [otf('cat', 'txt', 'cat_out.txt',)]

    def cmd(self, i, o, s, **kwargs):
        return 'cat {input} > {o[txt]}', {
            'input': ' '.join(map(str, i.format['txt']))
        }


class Paste(Tool):
    inputs = [itf(format='txt')]
    outputs = [otf('paste', 'txt', 'paste.txt')]

    def cmd(self, i, o, s, **kwargs):
        return 'paste {input} > {o[txt]}', {
            'input': ' '.join(map(str, i.format['txt']))
        }


class WordCount(Tool):
    inputs = [itf(format='txt')]
    outputs = [otf('wc', 'txt')]

    def cmd(self, i, o, s):
        return 'wc {input} > {o[txt]}', {
            'input': ' '.join(map(str, i.format['txt']))
        }


class Fail(Tool):
    def cmd(self, i, o, s, **kwargs):
        return '__fail__'


class MD5Sum(Tool):
    inputs = [itf(format='*')]
    outputs = [otf(format='md5', basename="{i.format[*].basename}.md5")]

    def cmd(self, i, o, s, **kwargs):
        return 'md5sum {inp}', dict(inp=" ".join(map(str, i.values())))
