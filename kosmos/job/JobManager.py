import os

opj = os.path.join
from ..util.helpers import mkdir
from .local import DRM_Local
from .lsf import DRM_LSF
from .. import TaskStatus, StageStatus, library_path
import time
import shutil


class JobManager(object):
    def __init__(self, get_drmaa_native_specification, default_drm='local'):
        if default_drm == 'local':
            self.default_drm = DRM_Local(self)
        elif default_drm == 'lsf':
            self.default_drm = DRM_LSF(self)
        else:
            raise ValueError("default_drm `%s` not supported.  "
                             "`default_drm` must be one of: 'local', 'lsf'" % default_drm)
        self.running_tasks = []
        self.get_drmaa_native_specification = get_drmaa_native_specification

    def submit(self, task):
        self.running_tasks.append(task)
        task.status = TaskStatus.waiting

        if task.NOOP:
            task.status = TaskStatus.submitted
        else:
            mkdir(task.output_dir)
            mkdir(task.log_dir)
            self.create_command_sh(task)
            task.drmaa_native_specification = self.get_drmaa_native_specification(self.default_drm.name, task)
            self.default_drm.submit_job(task)
            task.status = TaskStatus.submitted
            task.session.commit()

    def terminate(self):
        for task in self.running_tasks:
            self.default_drm.kill(task)
            task.status = TaskStatus.killed
            task.stage.status = StageStatus.killed


    def get_finished_tasks(self, at_least_one=True):
        """
        :returns: A completed task, or None if there are no tasks to wait for
        """
        if len(self.running_tasks):
            while True:
                noops = filter(lambda t: t.NOOP, self.running_tasks)
                non_noops = filter(lambda t: not t.NOOP, self.running_tasks)
                finished_tasks = self.default_drm.filter_is_done(non_noops) if len(non_noops) else []
                finished_tasks += noops
                if len(finished_tasks):
                    for task in finished_tasks:
                        self.running_tasks.remove(task)
                    return finished_tasks
                if at_least_one:
                    time.sleep(.1)
                else:
                    return []
        else:
            if at_least_one:
                raise AttributeError('No tasks are running, and `at_least_one` is set to True')
            return []

    def create_command_sh(self, task):
        """Create a sh script that will execute a command"""
        with open(task.output_command_script_path, 'wb') as f:
            f.write("#!/bin/bash\n")
            f.write(task.command + "\n")
        os.system('chmod 700 "{0}"'.format(task.output_command_script_path))

    def get_command_str(self, task):
        "The command to be stored in the command.sh script"
        return "python {profile} -f {profile_out} {command_script_path}".format(
            profile=os.path.join(library_path, 'profile/profile.py'),
            #db = task.profile_output_path+'.sqlite',
            profile_out=task.output_profile_path,
            command_script_path=task.output_command_script_path
        )

