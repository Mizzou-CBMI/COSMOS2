import subprocess as sp
import getpass
from collections import OrderedDict

from cosmos.job.drm.util import exit_process_group

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
        et = ET.fromstring(sp.check_output(['qstat', '-ext', '-xml', '-u', user], preexec_fn=exit_process_group))
    except (sp.CalledProcessError, OSError):
        # Error occurs if there are no jobs
        return pd.DataFrame()

    dicts = list( job_list_to_dict(jl) for jl in et.findall('.//job_list') )
    return pd.DataFrame.from_dict(dicts)[dicts[0].keys()]
