#!/usr/bin/env python
import imp
use_cuda_device = imp.load_source("module", "ex_gpu.py").use_cuda_device

# To use ipdb, uncomment the next two lines and tab over the function call
#import ipdb
#with ipdb.launch_ipdb_on_exception():
use_cuda_device(
**{ u'num_gpus': 2, u'some_arg': 3}
)

