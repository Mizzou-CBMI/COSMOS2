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
