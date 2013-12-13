from subprocess import Popen
import os

import psutil
import json

class DRM_Local():
    """
    Note there can only be one of these instantiated at a time
    """

    def __init__(self, workflow, jobmanager):
        self.workflow = workflow
        self.jobmanager = jobmanager

    def submit_job(self, jobAttempt):
        p = Popen(self.jobmanager._create_cmd_str(jobAttempt).split(' '),
                  stdout=open(jobAttempt.STDOUT_filepath, 'w'),
                  stderr=open(jobAttempt.STDERR_filepath, 'w'),
                  preexec_fn=preexec_function()
                  )
        jobAttempt.drmaa_jobID = p.pid
        jobAttempt.save()

    def poll(self,jobAttempt):
        try:
            p = psutil.Process(jobAttempt.drmaa_jobID)
            exit_code = p.wait(timeout=0)
            return exit_code
        except psutil.TimeoutExpired:
            pass
        except psutil.NoSuchProcess:
            profile_output = json.load(open(jobAttempt.profile_output_path, 'r'))
            exit_code = profile_output['exit_status']
            return exit_code

        return None

    @property
    def jobAttempts(self):
        return self.workflow.jobAttempts

    def status(self, jobAttempt):
        """
        Queries the DRM for the status of the job
        """
        if jobAttempt.queue_status == 'queued':
            return 'job is running'
        if jobAttempt.queue_status == 'completed':
            if jobAttempt.exit_status == 0:
                return 'job finished normally'
            else:
                return 'job finished, but failed'
        return 'has not been queued'


    def terminate(self, jobAttempt):
        "Terminates a jobAttempt"
        try:
            psutil.Process(jobAttempt.drmaa_jobID).kill()
        except psutil.NoSuchProcess:
            pass


def preexec_function():
    # Ignore the SIGINT signal by setting the handler to the standard
    # signal handler SIG_IGN.  This allows Cosmos to cleanly
    # terminate jobs when there is a ctrl+c event
    os.setpgrp()


class JobStatusError(Exception):
    pass
