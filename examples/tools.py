from cosmos import Tool, find, out_dir 
from main import settings as s


class Sleep(Tool):
    def cmd(self, time=10):
        return 'sleep %s' % time


class Echo(Tool):
    def cmd(self, word, out_txt=out_dir('echo.txt')):
        return '{s[echo_path]} {word} > {out_txt}'.format(s=s, **locals())


class Cat(Tool):
    def cmd(self, inputs=find('txt$', n='>=1'), out_txt=out_dir('cat.txt')):
        return 'cat {input_str} > {out_txt}'.format(
            input_str=' '.join(map(str, inputs)),
            **locals()
        )

class Paste(Tool):
    def cmd(self, input_txts=find('txt$', n='>=1'), out_txt=out_dir('paste.txt')):
        return 'paste {input} > {out_txt}'.format(
            input=' '.join(map(str, (input_txts,))),
            **locals()
        )


class WordCount(Tool):
    def cmd(self, chars=False, input_txts=find('txt$', n='>=1'), out_txt=out_dir('wc.txt')):
        c = ' -c' if chars else ''
        return 'wc{c} {input} > {out_txt}'.format(
            input=' '.join(map(str, input_txts)),
            **locals()
        )


class Fail(Tool):
    def cmd(self):
        return '__fail__'


class MD5Sum(Tool):
    def cmd(self, in_file=find('.*', n=1), out_md5=out_dir('checksum.md5')):
        out_md5.basename = in_file.basename + '.md5'
        return 'md5sum {in_file}'.format(**locals()) 
