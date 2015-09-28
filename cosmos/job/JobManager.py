import os

opj = os.path.join
from ..util.helpers import mkdir
from .drm.drm_local import DRM_Local
from .drm.drm_lsf import DRM_LSF
from .drm.drm_ge import DRM_GE
from .drm.drm_drmaa import DRM_DRMAA
from .. import TaskStatus, StageStatus, NOOP
import itertools as it
from operator import attrgetter
from ..core.cmd_fxn.signature import call


class JobManager(object):
    def __init__(self, cosmos_app, get_submit_args, default_queue=None, cmd_wrapper=None):
        self.cosmos_app = cosmos_app
        self.drms = dict(local=DRM_Local(self))  # always support local execution
        self.drms['lsf'] = DRM_LSF(self)
        self.drms['ge'] = DRM_GE(self)
        self.drms['drmaa'] = DRM_DRMAA(self)

        self.local_drm = DRM_Local(self)
        self.running_tasks = []
        self.get_submit_args = get_submit_args
        self.default_queue = default_queue
        self.cmd_wrapper = cmd_wrapper

    def call_cmd_fxn(self, task):
        """
        NOTE THIS METHOD MUST BE THREAD SAFE
        :param task:
        :return:
        """
        session = self.cosmos_app.session  # we expect this to be its own thread
        # thread_local_task = session.merge(task)
        thread_local_task = task

        if hasattr(task, 'cmd_fxn'):
            if self.cmd_wrapper:
                fxn = self.cmd_wrapper(thread_local_task, task.stage.name, task.input_map, task.output_map)(task.cmd_fxn)
            else:
                fxn = task.cmd_fxn

            command = call(fxn, thread_local_task, task.input_map, task.output_map)
        else:
            command = thread_local_task.tool._generate_command(thread_local_task)

        return command

    def submit_task(self, task, command):
        if command == NOOP:
            task.NOOP = True
        else:
            mkdir(task.log_dir)
            _create_command_sh(task, command)
            task.drm_native_specification = self.get_submit_args(task, default_queue=self.default_queue)
            assert task.drm is not None, 'task has no drm set'
            drm_jobID = self.drms[task.drm].submit_job(task)
            task.drm_jobID = drm_jobID

        task.status = TaskStatus.submitted

    def run_tasks(self, tasks):
        self.running_tasks += tasks

        # Run the cmd_fxns in parallel, but do not submit any jobs they return
        # Note we use the cosmos_app thread_pool here so we don't have to setup/teardown threads (or their sqlalchemy sessions)
        # commands = self.cosmos_app.thread_pool.map(self.call_cmd_fxn, tasks)
        commands = map(self.call_cmd_fxn, tasks)
        # commands = self.cosmos_app.futures_executor.map(self.call_cmd_fxn, tasks)

        # Submit the jobs in serial
        # TODO parallelize this for speed.  Means having all ORM stuff outside Job Submission.
        map(self.submit_task, tasks, commands)

    def terminate(self):
        f = lambda t: t.drm
        for drm, tasks in it.groupby(sorted(self.running_tasks, key=f), f):
            tasks = list(tasks)
            self.drms[drm].kill_tasks(tasks)
            for task in tasks:
                task.status = TaskStatus.killed
                task.stage.status = StageStatus.killed

    def get_finished_tasks(self):
        """
        :returns: A completed task, or None if there are no tasks to wait for
        """
        # NOOP tasks are already done
        for task in list(self.running_tasks):
            if task.NOOP:
                self.running_tasks.remove(task)
                yield task

        # For the rest, ask its DRM if it is done
        f = attrgetter('drm')
        for drm, tasks in it.groupby(sorted(self.running_tasks, key=f), f):
            for task, job_info_dict in self.drms[drm].filter_is_done(list(tasks)):
                self.running_tasks.remove(task)
                for k, v in job_info_dict.items():
                    setattr(task, k, v)
                yield task


def _create_command_sh(task, command):
    """Create a sh script that will execute a command"""
    with open(task.output_command_script_path, 'wb') as f:
        f.write(command)
    os.system('chmod 700 "{0}"'.format(task.output_command_script_path))
