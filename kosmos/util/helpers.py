import sys, pprint, re, logging, itertools
import subprocess as sp


def mkdir(path):
    sp.check_output('mkdir -p {0}'.format(path).split(' '))


def groupby(iterable, fxn):
    """aggregates an iterable using a function"""
    return itertools.groupby(sorted(iterable, key=fxn), fxn)


def kosmos_format(s, d):
    """
    Format()s string s with d.  If there is an error, print helpful messages .
    """
    if not isinstance(s, str):
        raise Exception('Wrapped function must return a str')
    try:
        return s.format(**d)
    except (KeyError, IndexError, TypeError) as e:
        print >> sys.stderr, "Format Error: {0}".format(e)
        print >> sys.stderr, "\tTried to format: {0}".format(pprint.pformat(s))
        print >> sys.stderr, "\tWith: {0}".format(pprint.pformat(d))
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
    return x


def formatError(txt, dict):
    """
    Prints a useful debugging message for a bad .format() call, then raises an exception
    """
    logging.warning('*' * 76)
    logging.warning("format() error occurred here:")
    logging.warning('txt is:\n' + txt)
    logging.warning('-' * 76)
    logging.warning('dict available is:\n' + pprint.pformat(dict, indent=4))

    logging.warning('*' * 76)
    raise Exception("Format() KeyError.  You did not pass the proper arguments to format() the txt.")


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
        fh = logging.FileHandler(path)
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(logging.Formatter('%(levelname)s: %(asctime)s: %(message)s', "%Y-%m-%d %H:%M:%S"))
        log.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter('%(levelname)s: %(asctime)s: %(message)s', "%Y-%m-%d %H:%M:%S"))
    log.addHandler(ch)

    return log

