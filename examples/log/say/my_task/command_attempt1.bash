#!/usr/bin/env python
import imp
say = imp.load_source("module", "ex3.py").say

# To use ipdb, uncomment the next two lines and tab over the function call
#import ipdb
#with ipdb.launch_ipdb_on_exception():
say(
**{ u'out_file': u'out.txt', u'text': u'Hello World'}
)

