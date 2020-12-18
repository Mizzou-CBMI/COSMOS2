import os
import re
import signal
import sys
import time
from concurrent.futures.thread import ThreadPoolExecutor
from random import shuffle

from cosmos.job.drm.DRM_Base import DRM
from cosmos.job.drm.util import exit_process_group
from cosmos.api import TaskStatus
from cosmos.util.helpers import progress_bar
from cosmos.constants import TERMINATION_SIGNALS


if os.name == "posix" and sys.version_info[0] < 3:
    import subprocess32 as subprocess
else:
    import subprocess

MAX_THREADS = 50


def parse_COSMOS_LOCAL_GPU_DEVICES(COSMOS_LOCAL_GPU_DEVICES=None):
    """
    >>> list(parse_COSMOS_LOCAL_GPU_DEVICES('1'))
    ['1']
    >>> list(parse_COSMOS_LOCAL_GPU_DEVICES('1,1'))
    ['1', '1']
    >>> list(parse_COSMOS_LOCAL_GPU_DEVICES('1,2x3'))
    ['1', '2.0', '2.1', '2.2']

    """
    if COSMOS_LOCAL_GPU_DEVICES is None:
        COSMOS_LOCAL_GPU_DEVICES = os.environ.get("COSMOS_LOCAL_GPU_DEVICES")

    if COSMOS_LOCAL_GPU_DEVICES is not None:
        for gpu_str in COSMOS_LOCAL_GPU_DEVICES.split(","):
            # support "0[1-5]" for 5 slots on gpu 0
            m = re.search(r"(\d+)x(\d+)", gpu_str)
            if m:
                gpu, n = m.groups()
                for i in range(int(n)):
                    yield f"{gpu}.{i}"
            else:
                yield gpu_str


class DRM_Local(DRM):
    name = "local"
    poll_interval = 0.3

    def __init__(self, log, workflow=None):
        self.procs = dict()
        self.gpus_on_system = list(parse_COSMOS_LOCAL_GPU_DEVICES())
        shuffle(self.gpus_on_system)

        self.task_id_to_gpus_used = dict()

        super(DRM_Local, self).__init__(log, workflow)

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
                f"Not enough gpus, need {task.gpu_req} gpus, "
                f"there are {len(self.gpus_on_system)} gpus available: {self.gpus_on_system}, "
                f"and gpus left are: {self.gpus_left}. "
                f"This can be caused by max_gpus not being set appropriately, you usually "
                f"want it to be set to the number of GPUs on the local machine if all of your GPU "
                f"jobs are using the local DRM.  "
                f"For example COSMOS_LOCAL_GPU_DEVICES=1,2,3 and --max-gpus 3.  "
            )

        self.task_id_to_gpus_used[task.id] = self.gpus_left[: task.gpu_req]

    def submit_job(self, task):
        raise NotImplementedError("use .submit_jobs()")

    def _submit_job(self, task):  # this is needed for multiprocessing
        if self.workflow.termination_signal not in TERMINATION_SIGNALS:
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
                env["CUDA_VISIBLE_DEVICES"] = ",".join(
                    re.sub("\.\d+?", "", gpu) for gpu in self.task_id_to_gpus_used[task.id]
                )

            # set additional environment variables
            if task.environment_variables is not None:
                for (k, v,) in task.environment_variables.items():
                    env[k] = v

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

            return drm_jobID
        else:
            return None

    def submit_jobs(self, tasks):
        if len(tasks) > 1:
            with ThreadPoolExecutor(min(len(tasks), MAX_THREADS)) as pool:
                rv = list(progress_bar(pool.map(self._submit_job, tasks), len(tasks), "Submitting"))
        else:
            # submit in serial without a progress bar
            rv = map(self._submit_job, tasks)

        for task, drm_jobID in zip(tasks, rv):
            if drm_jobID is not None:
                task.drm_jobID = drm_jobID
                task.status = TaskStatus.submitted
            else:
                self.procs[None] = None
                task.drm_jobID = None
                task.status = TaskStatus.killed

    def _is_done(self, task, timeout=0):
        try:
            p = self.procs[task.drm_jobID]
            p.wait(timeout=timeout)
            return True
        except (subprocess.TimeoutExpired, AttributeError):
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
