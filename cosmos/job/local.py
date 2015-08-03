import os

import psutil

from .drm import DRM
from .. import TaskStatus


class DRM_Local(DRM):
    name = 'local'

    def __init__(self, jobmanager):
        self.jobmanager = jobmanager
        self.procs = dict()

    def submit_job(self, task):

        p = psutil.Popen(task.output_command_script_path,
                         stdout=open(task.output_stderr_path, 'w'),
                         stderr=open(task.output_stdout_path, 'w'),
                         preexec_fn=preexec_function(),
                         shell=False,env=os.environ
        )
        task.drm_jobID = p.pid
        self.procs[task.drm_jobID] = p

    def _is_done(self, task):
        try:
            p = self.procs[task.drm_jobID]
            p.wait(timeout=0)
            return True
        except psutil.TimeoutExpired:
            return False
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

    def get_task_return_data(self, task):
        return dict(exit_status=self.procs[task.drm_jobID].wait(timeout=0))

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
