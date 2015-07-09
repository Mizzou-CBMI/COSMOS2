import os

opj = os.path.join
from ..util.helpers import mkdir
from .local import DRM_Local
from .lsf import DRM_LSF
from .ge import DRM_GE
from .. import TaskStatus, StageStatus, ExecutionStatus, NOOP
import itertools as it
from operator import attrgetter


class JobManager(object):
    def __init__(self, get_submit_args, default_queue=None):
        self.drms = dict(local=DRM_Local(self))  # always support local execution
        self.drms['lsf'] = DRM_LSF(self)
        self.drms['ge'] = DRM_GE(self)

        self.local_drm = DRM_Local(self)
        self.running_tasks = []
        self.get_submit_args = get_submit_args
        self.default_queue = default_queue


    def submit(self, task):
        self.running_tasks.append(task)
        task.status = TaskStatus.waiting

        command = task.tool._generate_command(task)

        if command == NOOP:
            task.NOOP = True
            task.status = TaskStatus.submitted
        else:
            mkdir(task.log_dir)
            self._create_command_sh(task, command)
            task.drm_native_specification = self.get_submit_args(task, default_queue=self.default_queue)
            assert task.drm is not None, 'task has no drm set'
            self.drms[task.drm].submit_job(task)
            task.status = TaskStatus.submitted

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
        for t in list(self.running_tasks):
            if t.NOOP:
                self.running_tasks.remove(t)
                yield t
        f = attrgetter('drm')
        for drm, tasks in it.groupby(sorted(self.running_tasks, key=f), f):
            for t in self.drms[drm].filter_is_done(list(tasks)):
                self.running_tasks.remove(t)
                try:
                    t.update_from_profile_output()
                except IOError as e:
                    t.log.info(e)
                    t.execution.status = ExecutionStatus.failed
                yield t

    def _create_command_sh(self, task, command):
        """Create a sh script that will execute a command"""
        with open(task.output_command_script_path, 'wb') as f:
            f.write('#!/bin/bash\n'
                    'set -e\n'
                    'set -o pipefail\n'
                    '\n'
                    + command + "\n")
        os.system('chmod 700 "{0}"'.format(task.output_command_script_path))

    def get_command_str(self, task):
        "The command to be stored in the command.sh script"
        p = "psprofile{skip_profile} -w 100 -o {profile_out} {command_script_path}".format(
            profile_out=task.output_profile_path,
            command_script_path=task.output_command_script_path,
            skip_profile=' --skip_profile' if task.skip_profile else ''
        )
        return p

