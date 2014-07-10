import os

opj = os.path.join
from ..util.helpers import mkdir
from .local import DRM_Local
from .lsf import DRM_LSF
from .ge import DRM_GE
from .. import TaskStatus, StageStatus, library_path
import itertools as it
from operator import attrgetter
import subprocess as sp

python_path = sp.check_output(['which', 'python']).strip()

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

        if task.NOOP:
            task.status = TaskStatus.submitted
        else:
            mkdir(task.output_dir)
            mkdir(task.log_dir)
            self._create_command_sh(task)
            task.drmaa_native_specification = self.get_submit_args(task.drm, task, default_queue=self.default_queue)
            assert task.drm is not None, 'task has no drm set'
            self.drms[task.drm].submit_job(task)
            task.status = TaskStatus.submitted
            #task.session.commit()

    def terminate(self):
        for task in self.running_tasks:
            self.drms[task.drm].kill(task)
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
                t.update_from_profile_output()
                yield t

    def _create_command_sh(self, task):
        """Create a sh script that will execute a command"""
        with open(task.output_command_script_path, 'wb') as f:
            f.write("#!/bin/bash\n")
            f.write(task.command + "\n")
        os.system('chmod 700 "{0}"'.format(task.output_command_script_path))

    def get_command_str(self, task):
        "The command to be stored in the command.sh script"
        p = "{python} {profile} -f {profile_out} {command_script_path}".format(
            python=python_path,
            profile=os.path.join(library_path, 'profile/profile.py'),
            # db = task.profile_output_path+'.sqlite',
            profile_out=task.output_profile_path,
            command_script_path=task.output_command_script_path
        )
        return p

