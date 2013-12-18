
import sys, pprint, re, logging, itertools
import subprocess as sp

def mkdir(path):
    sp.check_output('mkdir -p {0}'.format(path).split(' '))

def groupby(iterable, fxn):
    """aggregates an iterable using a function"""
    return itertools.groupby(sorted(iterable, key=fxn), fxn)


def cosmos_format(s, d):
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


def validate_is_type_or_list(variable, klass):
    if isinstance(variable, list) and (len(variable) == 0 or isinstance(variable[0], klass)):
        return variable
    elif isinstance(variable, klass):
        return [variable]
    else:
        raise TypeError, '{0} must be a list of {1} or a {1}'.format(variable, klass)


def validate_name(txt, field_name=''):
    """
    Validates that txt is alphanumeric and underscores, decimals, or hyphens only
    """
    if re.match('^[a-zA-Z0-9_\.-]+$', txt) == None:
        raise Exception(
            'Field {0} must be alphanumeric, periods, or hyphens only.  Text that failed: {1}'.format(field_name, txt))


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
