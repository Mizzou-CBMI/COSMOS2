from subprocess import Popen
import os

import psutil
from .drm import DRM

from .. import TaskStatus


class DRM_Local(DRM):
    name = 'local'

    def __init__(self, jobmanager):
        self.jobmanager = jobmanager

    def submit_job(self, task):
        p = Popen(self.jobmanager.get_command_str(task),
                  stdout=open(task.output_stderr_path, 'w'),
                  stderr=open(task.output_stdout_path, 'w'),
                  preexec_fn=preexec_function(),
                  shell=True
                  )
        task.drm_jobID = p.pid

    def _is_done(self, task):
        try:
            p = psutil.Process(task.drm_jobID)
            p.wait(timeout=0)
            return True
        except psutil.TimeoutExpired:
            pass
        except psutil.NoSuchProcess:
            # profile_output = json.load(open(task.output_profile_path, 'r'))
            # exit_code = profile_output['exit_status']
            return True

        return False

    def filter_is_done(self, tasks):
        return filter(self._is_done, tasks)


    def drm_statuses(self, tasks):
        """
        :returns: (dict) task.drm_jobID -> drm_status
        """

        def f(task):
            if task.drm_jobID is None:
                return '!'
            if task.status == TaskStatus.submitted:
                return 'Running'
            else:
                return ''

        return {task.drm_jobID: f(task) for task in tasks}

    def kill(self, task):
        "Terminates a task"

        try:
            psutil.Process(task.drm_jobID).kill()
        except psutil.NoSuchProcess:
            pass


    def kill_tasks(self, tasks):
        for t in tasks:
            self.kill(t)


def preexec_function():
    # Ignore the SIGINT signal by setting the handler to the standard
    # signal handler SIG_IGN.  This allows Cosmos to cleanly
    # terminate jobs when there is a ctrl+c event
    os.setpgrp()


class JobStatusError(Exception):
    pass
