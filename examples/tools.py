from cosmos import find, out_dir
from main import settings as s


def sleep(time=10):
    return 'sleep %s' % time


def echo(word, out_txt=out_dir('echo.txt')):
    return '{s[echo_path]} {word} > {out_txt}'.format(s=s, **locals())


def cat(inputs=find('txt$', n='>=1'), out_txt=out_dir('cat.txt')):
    return 'cat {input_str} > {out_txt}'.format(
        input_str=' '.join(map(str, inputs)),
        **locals()
    )


def paste(input_txts=find('txt$', n='>=1'), out_txt=out_dir('paste.txt')):
    return 'paste {input} > {out_txt}'.format(
        input=' '.join(map(str, (input_txts,))),
        **locals()
    )


def word_count(chars=False, input_txts=find('txt$', n='>=1'), out_txt=out_dir('wc.txt')):
    c = ' -c' if chars else ''
    return 'wc{c} {input} > {out_txt}'.format(
        input=' '.join(map(str, input_txts)),
        **locals()
    )


def fail():
    return '__fail__'


def md5sum(in_file=find('.*', n=1), out_md5=out_dir('checksum.md5')):
    out_md5.basename = in_file.basename + '.md5'
    return 'md5sum {in_file}'.format(**locals())
