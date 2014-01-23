import os

opj = os.path.join
from ..util.helpers import mkdir
from .local import DRM_Local
from .. import settings
from .. import TaskStatus, StageStatus
import time


class JobManager(object):
    def __init__(self):
        self.drm = DRM_Local(self)
        self.running_tasks = []

    def submit(self, task):
        self.running_tasks.append(task)
        task.status = TaskStatus.waiting

        # if task.profile.get('exit_status', None) == 0:
        #     task.status = TaskStatus.successful
        # else:

        if task.NOOP:
            task.status = TaskStatus.submitted
        else:
            mkdir(task.output_dir)
            mkdir(task.log_dir)
            self.create_command_sh(task)
            task.status = TaskStatus.submitted
            self.drm.submit_job(task)

    def terminate(self):
        for task in self.running_tasks:
            self.drm.kill(task)
            task.status = TaskStatus.killed
            task.stage.status = StageStatus.killed


    def get_finished_tasks(self, at_least_one=True):
        """
        :returns: A completed task, or None if there are no tasks to wait for
        """
        def task_is_finished(task):
            if task.NOOP:
                return True
            return self.drm.poll(task) is not None

        if len(self.running_tasks):
            while True:
                finished_tasks = filter(task_is_finished, self.running_tasks)
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
                raise AttributeError, 'No tasks are running, and at_least_one is set to True'
            return []

    def create_command_sh(self, task):
        """Create a sh script that will execute a command"""
        with open(task.output_command_script_path, 'wb') as f:
            f.write("#!/bin/bash\n")
            f.write(task.generate_cmd() + "\n")
        os.system('chmod 700 "{0}"'.format(task.output_command_script_path))

    def get_command_str(self, task):
        "The command to be stored in the command.sh script"
        return "python {profile} -f {profile_out} {command_script_path}".format(
            profile=os.path.join(settings['library_path'], 'profile/profile.py'),
            #db = task.profile_output_path+'.sqlite',
            profile_out=task.output_profile_path,
            command_script_path=task.output_command_script_path
        )