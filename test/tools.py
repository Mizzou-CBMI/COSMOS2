from kosmos.Tool import Tool
from kosmos.TaskFile import TaskFile

class Sleep(Tool):
    inputs = ['*']
    forward_input = True

    def cmd(self,i,o,s,**kwargs):
        return 'sleep 10'

class ECHO(Tool):
    outputs = ['txt']
    time_req = 1 #min
    
    def cmd (self,i,o,s,word):
        return 'echo {word} > {o[txt]}'
    
class CAT(Tool):
    inputs = ['txt']
    outputs = [TaskFile(name='txt',basename='cat.txt')]
    time_req = 1
    
    def cmd(self,i,o,s,**kwargs):
        return 'cat {input} > {o[txt]}', {
                'input':' '.join(map(lambda x: str(x),i['txt']))
                }
    
class PASTE(Tool):
    inputs = ['txt']
    outputs = [TaskFile(name='txt',basename='paste.txt')]
    time_req = 1
    
    def cmd(self,i,o,s,**kwargs):
        return 'paste {input} > {o[txt]}', {
                'input':' '.join(map(lambda x: str(x),i['txt']))
                }
    
class WC(Tool):
    inputs = ['txt']
    outputs = ['txt']
    time_req = 1

    default_para = { 'args': '' }
    
    def cmd(self,i,o,s,**kwargs):
        return 'wc {input} > {o[txt]}', {
                'input':' '.join(map(lambda x: str(x),i['txt']))
                }

class FAIL(Tool):
    outputs = ['txt']
    def cmd(self,i,o,s,**kwargs):

        return '{o[txt]} __fail__'

class MD5Sum(Tool):
    inputs = ['*']
    outputs = ['md5']

    def cmd(self,i,o,s,**kwargs):
        return 'md5sum {inp}', dict(inp=" ".join(map(lambda x: str(x), i)))
