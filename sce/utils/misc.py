import os
import re
from datetime import datetime

from starcluster.logger import log
from decorator import decorator
from inspect import getcallargs


def catchall(f,*args,**kwargs):
    """Catch all exceptions and return"""
    try:
        return f(*args,**kwargs)
    except Exception as e:
        log.error('Plugin unsuccessful!')
        log.exception(e)
        if hasattr(e,'exceptions'):
            for e2 in e.exceptions:
                if type(e2) == list:
                    for e3 in e2:
                        log.error(e3)
                else:
                    log.error(e2)

def _trace(f, *args, **kwargs):
    callargs = getcallargs.getcallargs(f,*args,**kwargs)
    del callargs['self']
    log.info(
        '{0}({1})'.format(
            f.__name__,
            ', '.join(
                map(lambda i: '{0[0]}={0[1]}'.format(i),callargs.items())
            )
        )
    )
    return f(*args, **kwargs)

def trace(f):
    """
    Automatically logs the decorated method and its parameters each time it is called.
    :param f: the method
    """
    return decorator(_trace, f)

