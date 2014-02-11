import sys, pprint, re, logging, itertools
import subprocess as sp
import signal
import os

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
    class TimeOutException(Exception): pass
    def timed_out(signum, frame):
        "called when stdin read times out"
        raise TimeOutException('Timed out')
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
            if ans in ['y','yes','Yes']:
                return True
            if ans in ['n','no','N']:
                return False
        except TimeOutException:
            print "Confirmation timed out after {0}s, returning default of '{1}'".format(timeout,'yes' if default else 'no')
            return default

def mkdir(path):
    sp.check_output('mkdir -p "{0}"'.format(path), shell=True)


def groupby(iterable, fxn):
    """aggregates an iterable using a function"""
    return itertools.groupby(sorted(iterable, key=fxn), fxn)


def kosmos_format(s, d):
    """
    Format()s string s with d.  If there is an error, print helpful messages .
    """
    try:
        return s.format(**d)
    except Exception as e:
        formatError(s, d)
        raise


def parse_cmd(txt, **kwargs):
    """removes empty lines and white spaces, and appends a \ to the end of every line.
    also .format()s with the **kwargs dictioanry"""
    try:
        x = txt.format(**kwargs)
        x = x.split('\n')
        x = map(lambda x: re.sub(r"\\$", '', x.strip()).strip(), x)
        x = filter(lambda x: not x == '', x)
        x = ' \\\n'.join(x)
    except (KeyError, TypeError):
        formatError(txt, kwargs)
        raise
    return x


def formatError(txt, dict):
    """
    Prints a useful debugging message for a bad .format() call, then raises an exception
    """
    s = "{star}\n" \
    "format() error:\n" \
    "txt:\n" \
    "{txt}\n" \
    "{dash}\n" \
    "{dic}\n" \
    "{star}\n".format(
        star='*' * 76,
        txt=txt,
        dash='-' * 76,
        dic=pprint.pformat(dict, indent=4))
    print s


def get_logger(name, path):
    """
    Gets a logger of name `name` that prints to stderr and to path

    :returns: (logger, True if the logger was initialized, else False)
    """
    log = logging.getLogger(name)
    #logging.basicConfig(level=logging.DEBUG)

    #check if we've already configured logger
    if len(log.handlers) > 0:
        return log

    log.setLevel(logging.INFO)
    # create file handler which logs debug messages
    if path:
        assert os.path.exists(os.path.dirname(path)), '%s does not exist' % path
        fh = logging.FileHandler(path)
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(logging.Formatter('%(levelname)s: %(asctime)s: %(message)s', "%Y-%m-%d %H:%M:%S"))
        log.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter('%(levelname)s: %(asctime)s: %(message)s', "%Y-%m-%d %H:%M:%S"))
    log.addHandler(ch)

    return log

