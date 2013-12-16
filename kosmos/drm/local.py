from subprocess import Popen
import os

import psutil
import json

class DRM_Local():
    """
    Note there can only be one of these instantiated at a time
    """

    def __init__(self, jobmanager):
        self.jobmanager = jobmanager

    def submit_job(self, task):
        p = Popen(self.jobmanager.get_command_str(task).split(' '),
                  stdout=open(task.output_stderr_path, 'w'),
                  stderr=open(task.output_stdout_path, 'w'),
                  preexec_fn=preexec_function()
                  )
        task.drmaa_jobID = p.pid

    def poll(self,task):
        try:
            p = psutil.Process(task.drmaa_jobID)
            exit_code = p.wait(timeout=0)
            return exit_code
        except psutil.TimeoutExpired:
            pass
        except psutil.NoSuchProcess:
            profile_output = json.load(open(task.output_profile_path, 'r'))
            exit_code = profile_output['exit_status']
            return exit_code

        return None

    @property
    def tasks(self):
        return self.workflow.tasks

    def status(self, task):
        """
        Queries the DRM for the status of the job
        """
        if task.queue_status == 'queued':
            return 'job is running'
        if task.queue_status == 'completed':
            if task.exit_status == 0:
                return 'job finished normally'
            else:
                return 'job finished, but failed'
        return 'has not been queued'


    def terminate(self, task):
        "Terminates a task"
        try:
            psutil.Process(task.drmaa_jobID).kill()
        except psutil.NoSuchProcess:
            pass


def preexec_function():
    # Ignore the SIGINT signal by setting the handler to the standard
    # signal handler SIG_IGN.  This allows Cosmos to cleanly
    # terminate jobs when there is a ctrl+c event
    os.setpgrp()


class JobStatusError(Exception):
    pass
