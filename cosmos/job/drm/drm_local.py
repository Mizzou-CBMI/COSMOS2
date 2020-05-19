import os
import signal
import sys
import time

from cosmos.job.drm.DRM_Base import DRM
from cosmos.job.drm.util import exit_process_group
from cosmos.api import TaskStatus


if os.name == "posix" and sys.version_info[0] < 3:
    import subprocess32 as subprocess
else:
    import subprocess


class DRM_Local(DRM):
    name = "local"
    poll_interval = 0.3

    def __init__(self, log):
        self.procs = dict()
        self.gpus_on_system = (
            os.environ["COSMOS_LOCAL_GPU_DEVICES"].split(",")
            if "COSMOS_LOCAL_GPU_DEVICES" in os.environ
            else []
        )
        self.task_id_to_gpus_used = dict()

        super(DRM_Local, self).__init__(log)

    @property
    def gpus_used(self):
        return [gpu for gpus in list(self.task_id_to_gpus_used.values()) for gpu in gpus]

    @property
    def gpus_left(self):
        return list(set(self.gpus_on_system) - set(self.gpus_used))

    def acquire_gpus(self, task):
        if task.gpu_req > len(self.gpus_left):
            # if 'COSMOS_LOCAL_GPU_DEVICES' not in os.environ:
            raise EnvironmentError(
                "Not enough system gpus, need {task.gpu_req} gpus, "
                "gpus on the system are: {self.gpus_on_system}, "
                "and gpus left are: {self.gpus_left}. "
                "This can be caused by max_gpus not being set appropriately, you usually "
                "want it to be set to the number of GPUs on the local machine if all of your GPU "
                "jobs are using the local DRM.  "
                "Note that local DRM requires the environment variable "
                "COSMOS_LOCAL_GPU_DEVICES set to a "
                "comma delimited list of GPU devices to use.  It should "
                "be the same format as CUDA_VISIBLE_DEVICES.  ".format(**locals())
            )

        self.task_id_to_gpus_used[task.id] = self.gpus_left[: task.gpu_req]

    def submit_job(self, task):
        if task.time_req is not None:
            cmd = [
                "/usr/bin/timeout",
                "-k",
                "10",
                str(task.time_req),
                task.output_command_script_path,
            ]
        else:
            cmd = task.output_command_script_path

        env = os.environ.copy()
        if task.gpu_req:
            # Note: workflow won't submit jobs unless there are enough gpus available
            self.acquire_gpus(task)
            env["CUDA_VISIBLE_DEVICES"] = ",".join(map(str, self.task_id_to_gpus_used[task.id]))

        p = subprocess.Popen(
            cmd,
            stdout=open(task.output_stdout_path, "w"),
            stderr=open(task.output_stderr_path, "w"),
            shell=False,
            env=env,
            preexec_fn=exit_process_group,
        )
        p.start_time = time.time()
        drm_jobID = str(p.pid)
        self.procs[drm_jobID] = p
        task.drm_jobID = drm_jobID
        task.status = TaskStatus.submitted

    def _is_done(self, task, timeout=0):
        try:
            p = self.procs[task.drm_jobID]
            p.wait(timeout=timeout)
            return True
        except subprocess.TimeoutExpired:
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
                return "!"
            if task.status == TaskStatus.submitted:
                return "Running"
            else:
                return ""

        return {task.drm_jobID: f(task) for task in tasks}

    def _get_task_return_data(self, task):
        return dict(
            exit_status=self.procs[task.drm_jobID].wait(timeout=0),
            wall_time=round(int(time.time() - self.procs[task.drm_jobID].start_time)),
        )

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

    def release_resources_after_completion(self, task):
        if task.gpu_req:
            self.task_id_to_gpus_used.pop(task.id)


class JobStatusError(Exception):
    pass
