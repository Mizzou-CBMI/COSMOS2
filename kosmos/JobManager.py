import os
opj = os.path.join
from .helpers import mkdir

class JobManager(object):

    def __init__(self):
        self.running_jobs = []

    def submit(self, task):
        self.running_jobs.append(task)

        mkdir(task.output_dir)
        mkdir(task.log_dir)
        self.create_command_sh(task)

        print 'submit %s' % (self.get_profile_cmd_str(task))


    def yield_finished_job(self):
        if len(self.running_jobs):
            t =  self.running_jobs.pop()
            yield t

    def create_command_sh(self, task):
        """Create a sh script that will execute a command"""
        with open(task.output_command_script_path,'wb') as f:
            f.write("#!/bin/bash\n")
            f.write(task.generate_cmd()+"\n")
        os.system('chmod 700 {0}'.format(task.output_command_script_path))

    def get_profile_cmd_str(self,task):
        from . import settings
        "The command to be stored in the command.sh script"
        return "python {profile} -f {profile_out} {command_script_path}".format(
            profile = os.path.join(settings['library_path'],'profile/profile.py'),
            #db = task.profile_output_path+'.sqlite',
            profile_out = task.output_profile_path,
            command_script_path = task.output_command_script_path
        )