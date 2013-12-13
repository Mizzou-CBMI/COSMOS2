from subprocess import Popen, PIPE
import os
import re

from cosmos.models.job.JobAttempt import JobAttempt


class JobStatusError(Exception):
    pass


all_processes = []
current_jobs = []

decode_lsf_state = dict([
    ('UNKWN', 'process status cannot be determined'),
    ('PEND', 'job is queued and active'),
    ('PSUSP', 'job suspended while pending'),
    ('RUN', 'job is running'),
    ('SSUSP', 'job is system suspended'),
    ('USUSP', 'job is user suspended'),
    ('DONE', 'job finished normally'),
    ('EXIT', 'job finished, but failed'),
])


def preexec_function():
    # Ignore the SIGINT signal by setting the handler to the standard
    # signal handler SIG_IGN.  This allows Cosmos to cleanly
    # terminate jobs when there is a ctrl+c event
    os.setpgrp()


def get_bjobs():
    """
    returns a dict keyed by lsf job ids, who's values are a dict of bjob
    information about the job
    """
    p = Popen(['bjobs', '-a'], stdout=PIPE)
    p.wait()
    lines = p.stdout.readlines()
    bjobs = {}
    header = re.split("\s\s+", lines[0])
    for l in lines[1:]:
        items = re.split("\s\s+", l)
        bjobs[items[0]] = dict(zip(header, items))
    return bjobs


class JobManager_LSF():
    """
    Note there can only be one of these instantiated at a time
    """
    def __init__(self, workflow):
        self.workflow = workflow

    def _submit_job(self, jobAttempt):
        bsub = 'bsub -o {stdout} -e {stderr} {ns}'.format(
            stdout=jobAttempt.STDOUT_filepath,
            stderr=jobAttempt.STDERR_filepath,
            ns=jobAttempt.drmaa_native_specification)
        p = Popen((bsub + self._create_cmd_str(jobAttempt)).split(' '),
                  stdout=PIPE,
                  env=os.environ,
                  stderr=PIPE,
                  preexec_fn=preexec_function())
        p.wait()
        lsf_id = re.search('Job <(\d+)>', p.stdout.read()).group(1)
        jobAttempt.drmaa_jobID = lsf_id
        current_jobs.append(lsf_id)
        all_processes.append(lsf_id)

    def _check_for_finished_job(self):
        bjobs = get_bjobs()
        for id in current_jobs:
            status = bjobs[id]['STAT']
            if status in ['DONE', 'EXIT', 'UNKWN', 'ZOMBI']:
                current_jobs.remove(id)
                ja = JobAttempt.objects.get(drmaa_jobID=id)
                successful = True if status == 'DONE' else False
                ja._hasFinished(successful, bjobs[id])
                return ja
        return None


    def get_jobAttempt_status(self, jobAttempt):
        """
        Queries the DRM for the status of the job
        """
        try:
            bjob = get_bjobs()[jobAttempt.drmaa_jobID]
            return decode_lsf_state[bjob['STAT']]
        except Exception:
            'unknown'


    def terminate_jobAttempt(self, jobAttempt):
        "Terminates a jobAttempt"
        os.system('bkill {0}'.format(jobAttempt.drmaa_jobID))

