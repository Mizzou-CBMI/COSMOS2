from cosmos import Tool, abstract_input_taskfile as inp, abstract_output_taskfile as out
from main import settings as s


class Sleep(Tool):
    def cmd(self, time=10):
        return 'sleep %s' % time


class Echo(Tool):
    def cmd(self, word, out_txt=out('echo.txt')):
        return '{s[echo_path]} {word} > {out_txt}'.format(s=s, **locals())


class Cat(Tool):
    def cmd(self, inputs=inp(format='txt', n='>=1'), out_txt=out('cat.txt')):
        return 'cat {input_str} > {out_txt}'.format(
            input_str=' '.join(map(str, inputs)),
            **locals()
        )

class Paste(Tool):
    def cmd(self, input_txts=inp(format='txt', n='>=1'), out_txt=out('paste.txt')):
        return 'paste {input} > {out_txt}'.format(
            input=' '.join(map(str, (input_txts,))),
            **locals()
        )


class WordCount(Tool):
    def cmd(self, chars=False, input_txts=inp(format='txt', n='>=1'), out_txt=out('wc.txt')):
        c = ' -c' if chars else ''
        return 'wc{c} {input} > {out_txt}'.format(
            input=' '.join(map(str, input_txts)),
            **locals()
        )


class Fail(Tool):
    def cmd(self):
        return '__fail__'


class MD5Sum(Tool):
    inputs = []
    outputs = []

    def cmd(self, in_file=inp(format='*', n=1), out_md5=out('checksum.md5')):
        out_md5.basename = in_file.basename + '.md5'
        return 'md5sum {in_file}'.format(**locals()) 
