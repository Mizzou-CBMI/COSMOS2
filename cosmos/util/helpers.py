import pprint
import logging
import itertools as it
import subprocess as sp
import signal
import os
import random
import string

import time

def random_str(n):
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(n))

def make_dict(*list_of_dicts, **additional_kwargs):
    """
    :param list_of_dicts: a list of dicts, or Tasks (for Tasks, their tags will be used)
    :param additional_kwargs: extra key/vals to add to the dict
    :return: a merge of all the dicts in args and kwargs
    """
    r = dict()
    for elem in list_of_dicts:
        if not isinstance(elem, dict):
            raise '%s is not a dict' % elem
        r.update(elem)
    r.update(additional_kwargs)
    return r


def wait_for_file(execution, path, timeout=60, error=True):
    # Sometimes on a shared filesystem it can take a while for a file to propagate (i.e. eventual consistency)
    start = time.time()
    while not os.path.exists(path):
        time.sleep(.1)
        if time.time() - start > timeout:
            if error:
                execution.terminate(due_to_failure=True)
                raise IOError('giving up on %s existing' % path)
            else:
                return False
    return True


def has_duplicates(alist):
    return len(alist) != len(set(alist))


#
#
# def dag_descendants(node):
# """
# :param node: A Task or Stage instance
# :yields: a list of descendent task or stages
#
# This code is really simple because there are no cycles.
#     """
#     for c in node.children:
#         for n in dag_descendants(c):
#             yield n
#     yield node

def confirm(prompt=None, default=False, timeout=0):
    """prompts for yes or no defaultonse from the user. Returns True for yes and
    False for no.

    'default' should be set to the default value assumed by the caller when
    user simply types ENTER.

    :param timeout: (int) If set, prompt will return default.

    >>> confirm(prompt='Create Directory?', default=True)
    Create Directory? [y]|n:
    True
    >>> confirm(prompt='Create Directory?', default=False)
    Create Directory? [n]|y:
    False
    >>> confirm(prompt='Create Directory?', default=False)
    Create Directory? [n]|y: y
    True
    """

    class TimeOutException(Exception):
        pass

    def timed_out(signum, frame):
        "called when stdin read times out_dir"
        raise TimeOutException('Timed out_dir')

    signal.signal(signal.SIGALRM, timed_out)

    if prompt is None:
        prompt = 'Confirm'

    if default:
        prompt = '%s [%s]|%s: ' % (prompt, 'y', 'n')
    else:
        prompt = '%s [%s]|%s: ' % (prompt, 'n', 'y')

    while True:
        signal.alarm(timeout)
        try:
            ans = raw_input(prompt)
            signal.alarm(0)
            if not ans:
                return default
            if ans not in ['y', 'Y', 'yes', 'n', 'no', 'N']:
                print 'please enter y or n.'
                continue
            if ans in ['y', 'yes', 'Yes']:
                return True
            if ans in ['n', 'no', 'N']:
                return False
        except TimeOutException:
            print "Confirmation timed out_dir after {0}s, returning default of '{1}'".format(timeout,
                                                                                             'yes' if default else 'no')
            return default


#created_already = set()
def mkdir(path):
    global created_already
    #sp.check_output('mkdir -p "{0}"'.format(path), shell=True)
    if not os.path.exists(path):
        #created_already.add(path)
        os.makedirs(path)


def isgenerator(iterable):
    return hasattr(iterable, '__iter__') and not hasattr(iterable, '__len__')


def groupby2(iterable, fxn):
    """Sorts and aggregates an iterable using a function"""
    return it.groupby(sorted(iterable, key=fxn), fxn)


def duplicates(iterable):
    """return a list of duplicates"""
    for x, group in it.groupby(sorted(iterable)):
        if len(list(group)) > 1:
            yield x


def str_format(s, d, error_text=''):
    """
    Format()s string s with d.  If there is an error, print helpful message.
    """
    try:
        return s.format(**d)
    except Exception as e:
        formatError(s, d, error_text)
        raise


def strip_lines(txt):
    """strip()s txt as a whole, and each line in txt"""
    return '\n'.join(map(lambda s: s.strip(), txt.strip().split('\n')))


def formatError(txt, dict, error_text=''):
    """
    Prints a useful debugging message for a bad .format() call, then raises an exception
    """
    s = "{star}\n" \
        "format() error:\n" \
        "{error_text}" \
        "txt:\n" \
        "{txt}\n" \
        "{dash}\n" \
        "{dic}\n" \
        "{star}\n".format(
        star='*' * 76,
        txt=txt,
        dash='-' * 76,
        dic=pprint.pformat(dict, indent=4),
        error_text=error_text + "\n")
    print s


def get_logger(name, path):
    """
    Gets a logger of name `name` that prints to stderr and to root_path

    :returns: (logger, True if the logger was initialized, else False)
    """
    log = logging.getLogger(name)
    log.propagate = False
    #logging.basicConfig(level=logging.DEBUG)

    #check if we've already configured logger
    if len(log.handlers) > 0:
        return log

    log.setLevel(logging.DEBUG)
    # create file handler which logs debug messages
    if path:
        assert os.path.exists(os.path.dirname(path)), '%s does not exist' % os.path.dirname(path)
        fh = logging.FileHandler(path)
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(logging.Formatter('%(levelname)s: %(asctime)s: %(message)s', "%Y-%m-%d %H:%M:%S"))
        log.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(logging.Formatter('%(levelname)s: %(asctime)s: %(message)s', "%Y-%m-%d %H:%M:%S"))
    log.addHandler(ch)

    return log

