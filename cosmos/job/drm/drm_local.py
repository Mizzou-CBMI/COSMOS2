import os
import signal
import sys
if sys.version_info < (3,):
    import subprocess32 as sp
else:
    import subprocess as sp
import time

from cosmos.job.drm.DRM_Base import DRM
from cosmos.job.drm.util import exit_process_group
from cosmos.api import TaskStatus


class DRM_Local(DRM):
    name = 'local'
    poll_interval = 0.3

    def __init__(self, jobmanager):
        self.procs = dict()
        super(DRM_Local, self).__init__(jobmanager)

    def submit_job(self, task):

        if task.time_req is not None:
            cmd = ['/usr/bin/timeout', '-k', '10', str(task.time_req), task.output_command_script_path]
        else:
            cmd = task.output_command_script_path

        p = sp.Popen(cmd,
                     stdout=open(task.output_stdout_path, 'w'),
                     stderr=open(task.output_stderr_path, 'w'),
                     shell=False, env=os.environ,
                     preexec_fn=exit_process_group)
        p.start_time = time.time()
        drm_jobID = unicode(p.pid)
        self.procs[drm_jobID] = p
        task.drm_jobID = drm_jobID
        task.status = TaskStatus.submitted

    def _is_done(self, task, timeout=0):
        try:
            p = self.procs[task.drm_jobID]
            p.wait(timeout=timeout)
            return True
        except sp.TimeoutExpired:
            return False

        return False

    def filter_is_done(self, tasks):
        for t in tasks:
            if self._is_done(t):
                yield t, self._get_task_return_data(t)

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

    def _get_task_return_data(self, task):
        return dict(exit_status=self.procs[task.drm_jobID].wait(timeout=0),
                    wall_time=round(int(time.time() - self.procs[task.drm_jobID].start_time)))

    @staticmethod
    def _signal(task, sig):
        """Send the signal to a task and its child (background or pipe) processes."""
        try:
            pgid = os.getpgid(int(task.drm_jobID))
            os.kill(int(task.drm_jobID), sig)
            task.log.info("%s sent signal %s to pid %s" % (task, sig, task.drm_jobID))
            os.killpg(pgid, sig)
            task.log.info("%s sent signal %s to pgid %s" % (task, sig, pgid))
        except OSError:
            pass

    def kill_tasks(self, tasks):
        """
        Progressively send stronger kill signals to the specified tasks.
        """
        for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGKILL):
            signaled_tasks = []
            for t in tasks:
                if not self._is_done(t):
                    self._signal(t, sig)
                    signaled_tasks.append(t)

            max_tm = 10 + time.time()
            while signaled_tasks and time.time() < max_tm:
                task = signaled_tasks[0]
                if self._is_done(task, max_tm - time.time()):
                    task.log.info("%s confirmed exit after receiving signal %s" % (task, sig))
                    del signaled_tasks[0]

            if not signaled_tasks:
                break

        for t in tasks:
            if not self._is_done(t):
                t.log.warning("%s is still running locally!", t)

    def kill(self, task):
        return self.kill_tasks([task])


class JobStatusError(Exception):
    pass
