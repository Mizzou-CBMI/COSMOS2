import subprocess as sp
import os
import getpass
from collections import OrderedDict

from xml import etree
import xml.etree.ElementTree as ET

def qstat(user=getpass.getuser()):
    import pandas as pd
    """
    returns a dict keyed by lsf job ids, who's values are a dict of bjob
    information about the job
    """

    def job_list_to_dict(jl):
        d = OrderedDict((c.tag, c.text) for c in jl.getchildren())
        d.update(jl.attrib)
        return d

    try:
        et = ET.fromstring(sp.check_output(['qstat', '-ext', '-xml', '-u', user], preexec_fn=preexec_function))
    except (sp.CalledProcessError, OSError):
        # Error occurs if there are no jobs
        return pd.DataFrame()

    dicts = list( job_list_to_dict(jl) for jl in et.findall('.//job_list') )
    return pd.DataFrame.from_dict(dicts)[dicts[0].keys()]


def preexec_function():
    # Ignore the SIGINT signal by setting the handler to the standard
    # signal handler SIG_IGN.  This allows Cosmos to cleanly
    # terminate jobs when there is a ctrl+c event
    os.setpgrp()
    return os.setsid