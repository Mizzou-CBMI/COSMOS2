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


    def wait_for_a_job_to_finish(self):
        """
        :returns: A completed task, or None if there are no tasks to wait for
        """
        if len(self.running_tasks) == 0:
            return None
        while True:
            for task in self.running_tasks:
                if task.NOOP:
                    self.running_tasks.remove(task)
                    return task
                else:
                    r = self.drm.poll(task)
                    if r is not None:
                        self.running_tasks.remove(task)
                        return task
            time.sleep(.1)

    def create_command_sh(self, task):
        """Create a sh script that will execute a command"""
        with open(task.output_command_script_path, 'wb') as f:
            f.write("#!/bin/bash\n")
            f.write(task.generate_cmd() + "\n")
        os.system('chmod 700 {0}'.format(task.output_command_script_path))

    def get_command_str(self, task):
        "The command to be stored in the command.sh script"
        return "python {profile} -f {profile_out} {command_script_path}".format(
            profile=os.path.join(settings['library_path'], 'profile/profile.py'),
            #db = task.profile_output_path+'.sqlite',
            profile_out=task.output_profile_path,
            command_script_path=task.output_command_script_path
        )