from kosmos import Tool


class Sleep(Tool):
    inputs = ['*']

    def cmd(self, i, o, s):
        return 'sleep 10'


class Echo(Tool):
    outputs = ['txt']

    def cmd(self, i, o, s, word):
        return 'echo {word} > {o[txt]}'


class Cat(Tool):
    inputs = ['txt']
    outputs = [('txt', 'cat.txt',)]

    def cmd(self, i, o, s, **kwargs):
        return 'cat {input} > {o[txt]}', {
            'input': ' '.join(map(str, i['txt']))
        }


class Paste(Tool):
    inputs = ['txt']
    outputs = [('txt', 'paste.txt')]

    def cmd(self, i, o, s, **kwargs):
        return 'paste {input} > {o[txt]}', {
            'input': ' '.join(map(str, i['txt']))
        }


class WordCount(Tool):
    inputs = ['txt']
    outputs = ['txt']

    def cmd(self, i, o, s):
        return 'wc {input} > {o[txt]}', {
            'input': ' '.join(map(str, i['txt']))
        }


class Fail(Tool):
    def cmd(self, i, o, s, **kwargs):
        return '__fail__'


class MD5Sum(Tool):
    inputs = ['*']
    outputs = ['md5']

    def cmd(self, i, o, s, **kwargs):
        return 'md5sum {inp}', dict(inp=" ".join(map(str, i)))
