import os
import psutil
import signal
import time

from .DRM_Base import DRM
from .util import exit_process_group
from ...api import TaskStatus
from ...util.signal_handlers import sleep_through_signals


class DRM_Local(DRM):
    name = 'local'
    poll_interval = 0.3

    def __init__(self, jobmanager):
        self.jobmanager = jobmanager
        self.procs = dict()

    def submit_job(self, task):

        p = psutil.Popen(task.output_command_script_path,
                         stdout=open(task.output_stderr_path, 'w'),
                         stderr=open(task.output_stdout_path, 'w'),
                         shell=False, env=os.environ,
                         preexec_fn=exit_process_group)
        p.start_time = time.time()
        drm_jobID = unicode(p.pid)
        self.procs[drm_jobID] = p
        task.drm_jobID = drm_jobID
        task.status = TaskStatus.submitted

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
                    wall_time=time.time() - self.procs[task.drm_jobID].start_time)

    def _signal(self, task, sig):
        """Send a signal to a local task and any child (background or pipe) processes."""
        try:
            pgid = os.getpgid(int(task.drm_jobID))
            os.kill(int(task.drm_jobID), sig)
            task.log.info("%s sent signal %s to pid %s" % (task, sig, task.drm_jobID))
            os.killpg(pgid, sig)
            task.log.info("%s sent signal %s to pgid %s" % (task, pgid, task.drm_jobID))
        except OSError:
            pass

    def interrupt(self, task):
        """Terminate a task using SIGINT."""
        self._signal(task, signal.SIGINT)

    def terminate(self, task):
        """Terminate a task using SIGTERM."""
        self._signal(task, signal.SIGTERM)

    def kill(self, task):
        """Kill a task using SIGKILL."""
        self._signal(task, signal.SIGKILL)

    def kill_tasks(self, tasks):
        for t in tasks:
            self.interrupt(t)
        sleep_through_signals(10)
        for t in tasks:
            self.terminate(t)
        sleep_through_signals(10)
        for t in tasks:
            self.kill(t)


class JobStatusError(Exception):
    pass
