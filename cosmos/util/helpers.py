import itertools as it
import logging
import os
import pprint
import random
import shutil
import signal
import string
import sys
import tempfile
import time
from contextlib import contextmanager


def progress_bar(iterable, count=None, prefix="", progress_bar_size=60, output_file=sys.stdout):
    """
    Makes a progress bar that looks like:
    [#################...........] 100000/100000


    :param iterable: any iterable
    :param count: total size of iterable.  Only required if iterable does not have len() defined.
    :param prefix: prefix to add to the bar
    :param progress_bar_size: Number of characters for the progress bar
    :param output_file: output file to write to.  Defaults to stdout.
    :return:
    """
    if count is None:
        count = len(iterable)

    if prefix:
        prefix += " "

    last_num_hashes = None
    for i, item in enumerate(iterable):
        yield item
        num_hashes = int(progress_bar_size * (i + 1) / count)
        if num_hashes != last_num_hashes:
            hashes = "#" * num_hashes
            dots = "." * (progress_bar_size - num_hashes)
            done = i + 1
            output_file.write("{prefix}[{hashes}{dots}] {done}/{count}\r".format(**locals()))
            output_file.flush()

        last_num_hashes = num_hashes

    output_file.write("\n")
    output_file.flush()


@contextmanager
def temp_cwd():
    oldpath = os.getcwd()
    newpath = tempfile.mkdtemp()
    os.chdir(newpath)
    yield newpath

    os.chdir(oldpath)
    shutil.rmtree(newpath)


@contextmanager
def environment_variables(**kwargs):
    old_env_vars = {key: os.environ.get(key) for key in kwargs if key in os.environ}
    os.environ.update(kwargs)
    yield
    os.environ.update(old_env_vars)


def isinstance_namedtuple(x):
    t = type(x)
    b = t.__bases__
    if len(b) != 1 or b[0] != tuple:
        return False
    f = getattr(t, "_fields", None)
    if not isinstance(f, tuple):
        return False
    return all(type(n) == str for n in f)


def random_str(n):
    return "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(n))


def make_dict(*list_of_dicts, **additional_kwargs):
    """
    :param list_of_dicts: a list of dicts, or Tasks (for Tasks, their params will be used)
    :param additional_kwargs: extra key/vals to add to the dict
    :return: a merge of all the dicts in params and kwargs
    """
    r = dict()
    for elem in list_of_dicts:
        if not isinstance(elem, dict):
            raise "%s is not a dict" % elem
        r.update(elem)
    r.update(additional_kwargs)
    return r


def wait_for_file(workflow, path, timeout=60, error=True):
    # Sometimes on a shared filesystem it can take a while for a file to propagate (i.e. eventual consistency)
    start = time.time()
    while not os.path.exists(path):
        time.sleep(0.1)
        if time.time() - start > timeout:
            if error:
                workflow.terminate(due_to_failure=True)
                raise IOError("giving up on %s existing" % path)
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

    """

    class TimeOutException(Exception):
        pass

    def timed_out(signum, frame):
        "called when stdin read times out_dir"
        raise TimeOutException("Timed out_dir")

    signal.signal(signal.SIGALRM, timed_out)

    if prompt is None:
        prompt = "Confirm"

    if default:
        prompt = "%s [%s]|%s: " % (prompt, "y", "n")
    else:
        prompt = "%s [%s]|%s: " % (prompt, "n", "y")

    while True:
        signal.alarm(timeout)
        try:
            ans = input(prompt)
            signal.alarm(0)
            if not ans:
                return default
            if ans not in ["y", "Y", "yes", "n", "no", "N"]:
                print()
                "please enter y or n."
                continue
            if ans in ["y", "yes", "Yes"]:
                return True
            if ans in ["n", "no", "N"]:
                return False
        except TimeOutException:
            print()
            "Confirmation timed out_dir after {0}s, returning default of '{1}'".format(
                timeout, "yes" if default else "no"
            )
            return default


def mkdir(path):
    if path and not os.path.exists(path):
        os.makedirs(path)


def isgenerator(iterable):
    return hasattr(iterable, "__iter__") and not hasattr(iterable, "__len__")


def groupby2(iterable, fxn):
    """Sorts and aggregates an iterable using a function"""
    return it.groupby(sorted(iterable, key=fxn), fxn)


def duplicates(iterable):
    """return a list of duplicates"""
    for x, group in it.groupby(sorted(iterable)):
        if len(list(group)) > 1:
            yield x


def str_format(s, d, error_text=""):
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
    return "\n".join([s.strip() for s in txt.strip().split("\n")])


def formatError(txt, dict, error_text=""):
    """
    Prints a useful debugging message for a bad .format() call, then raises an exception
    """
    s = (
        "{star}\n"
        "format() error:\n"
        "{error_text}"
        "txt:\n"
        "{txt}\n"
        "{dash}\n"
        "{dic}\n"
        "{star}\n".format(
            star="*" * 76,
            txt=txt,
            dash="-" * 76,
            dic=pprint.pformat(dict, indent=4),
            error_text=error_text + "\n",
        )
    )
    print()
    s


def get_logger(name, path=None):
    """
    Gets a logger of name `name` that prints to stderr and to root_path

    :returns: (logger, True if the logger was initialized, else False)
    """
    log = logging.getLogger(name)
    log.propagate = False
    # logging.basicConfig(level=logging.DEBUG)

    # check if we've already configured logger
    if len(log.handlers) > 0:
        return log

    log.setLevel(logging.DEBUG)
    # create file handler which logs debug messages
    if path:
        d = os.path.dirname(path)
        assert d == "" or os.path.exists(d), "Cannot write to %s from %s" % (path, os.getcwd(),)
        fh = logging.FileHandler(path)
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(logging.Formatter("%(levelname)s: %(asctime)s: %(message)s", "%Y-%m-%d %H:%M:%S"))
        log.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(logging.Formatter("%(levelname)s: %(asctime)s: %(message)s", "%Y-%m-%d %H:%M:%S"))
    log.addHandler(ch)

    return log


def derive_exit_code_from_workflow(workflow):
    """
    Return an integer suitable for use as a CLI script's exit code.
    """
    if workflow.successful:
        return 0

    # if killed by a signal, return the signal number that terminated the workflow
    if workflow.termination_signal:
        return workflow.termination_signal

    # otherwise return the exit code of the first failed job that provided one
    ft = workflow.get_first_failed_task()
    if ft is not None:
        return ft.exit_status

    workflow.log.warning("%s unable to pinpoint cause of failure, returning %d", workflow, os.EX_SOFTWARE)
    return os.EX_SOFTWARE
