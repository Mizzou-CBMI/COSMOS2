

def sleep(time=10):
    return 'sleep %s' % time


def echo(word, out_txt):
    return 'echo {word} > {out_txt}'.format(**locals())


def cat(in_txts, out_txt):
    return 'cat {input_str} > {out_txt}'.format(
            input_str=' '.join(map(str, in_txts)),
            **locals()
    )


def paste(in_txts, out_txt):
    return 'paste {input} > {out_txt}'.format(
            input=' '.join(map(str, (in_txts,))),
            **locals()
    )


def word_count(in_txts, out_txt, chars=False):
    c = ' -c' if chars else ''
    return 'wc{c} {input} > {out_txt}'.format(
            input=' '.join(map(str, in_txts)),
            **locals()
    )


def fail():
    return '__fail__'


def md5sum(in_file, out_md5=None):
    if out_md5 is None:
        out_md5 = in_file + '.md5'
    return 'md5sum {in_file}'.format(**locals())
