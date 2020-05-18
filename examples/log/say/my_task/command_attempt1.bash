#!/usr/bin/env python
import logging
DEFAULT_LOG_FORMAT = "[%(name)s : %(asctime)-15s %(filename)s - %(funcName)s() ] %(message)s"
logging.basicConfig(format=DEFAULT_LOG_FORMAT, datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)

from examples.ex3 import say

# To use ipdb, uncomment the next two lines and tab over the function call
#import ipdb
#with ipdb.launch_ipdb_on_exception():
say(
**{'out_file': 'out.txt', 'text': 'Hello World'}
)

