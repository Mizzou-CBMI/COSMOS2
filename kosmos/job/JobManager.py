import os

opj = os.path.join
from ..util.helpers import mkdir
from .local import DRM_Local
from .lsf import DRM_LSF
from .. import TaskStatus, StageStatus, library_path
import time


class JobManager(object):
    def __init__(self, get_drmaa_native_specification, drm='local'):
        self.default_drm_name = drm
        self.always_local_drm = DRM_Local(self)
        self.running_tasks = []
        self.get_drmaa_native_specification = get_drmaa_native_specification

        if drm == 'local':
            self.drm = DRM_Local(self)
        elif drm == 'lsf':
            self.drm = DRM_LSF(self)
        else:
            raise ValueError("default_drm `%s` not supported. `default_drm` must be one of: 'local', 'lsf'" % drm)

    def submit(self, task):
        self.running_tasks.append(task)
        task.status = TaskStatus.waiting

        if task.NOOP:
            task.status = TaskStatus.submitted
        else:
            mkdir(task.output_dir)
            mkdir(task.log_dir)
            self._create_command_sh(task)
            task.drmaa_native_specification = self.get_drmaa_native_specification(self.drm.name, task)
            if task.always_local:
                task.drm = 'always_local'
                self.always_local_drm.submit_job(task)
            else:
                task.drm = self.default_drm_name
                self.drm.submit_job(task)
            task.status = TaskStatus.submitted
            task.session.commit()

    def terminate(self):
        for task in self.running_tasks:
            if task.drm == 'always_local':
                self.always_local_drm.kill(task)
            else:
                self.drm.kill(task)
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
        for t in self.filter_is_done(list(self.running_tasks)):
            self.running_tasks.remove(t)
            yield t


        # if len(self.running_tasks):
        #     while True:
        #         noops = filter(lambda t: t.NOOP, self.running_tasks)
        #         non_noops = filter(lambda t: not t.NOOP, self.running_tasks)
        #         finished_tasks = self.filter_is_done(non_noops)
        #         finished_tasks += noops
        #         if len(finished_tasks):
        #             for task in finished_tasks:
        #                 self.running_tasks.remove(task)
        #             return finished_tasks
        #         if at_least_one:
        #             time.sleep(.1)
        #         else:
        #             return []
        # else:
        #     if at_least_one:
        #         raise AttributeError('No tasks are running, and `at_least_one` is set to True')
        #     return []

    def filter_is_done(self, tasks):
        noops = []
        always_locals = []
        drms = []
        for t in tasks:
            if t.NOOP:
                noops.append(t)
            elif t.drm == 'always_local':
                always_locals.append(t)
            else:
                drms.append(t)

        return noops + self.always_local_drm.filter_is_done(always_locals) + self.drm.filter_is_done(drms)

    def _create_command_sh(self, task):
        """Create a sh script that will execute a command"""
        with open(task.output_command_script_path, 'wb') as f:
            f.write("#!/bin/bash\n")
            f.write(task.command + "\n")
        os.system('chmod 700 "{0}"'.format(task.output_command_script_path))

    def get_command_str(self, task):
        "The command to be stored in the command.sh script"
        sleep = 'sleep 120 && ' if task.drm not in ['local', 'always_local'] else ''
        sleep=''
        return "{sleep}python {profile} -f {profile_out} {command_script_path}".format(
            sleep=sleep,
            profile=os.path.join(library_path, 'profile/profile.py'),
            #db = task.profile_output_path+'.sqlite',
            profile_out=task.output_profile_path,
            command_script_path=task.output_command_script_path
        )

