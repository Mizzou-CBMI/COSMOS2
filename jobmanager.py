from django.db import models
from django.utils import timezone
import os,sys
from .JobAttempt import JobAttempt
from cosmos.utils.helpers import check_and_create_output_dir
from cosmos.utils.helpers import spinning_cursor
import time


from drm_local import DRM_Local
from jobmanager_lsf import JobManager_LSF

class JobStatusError(Exception):
    pass

class JobManager(models.Model):
    """
    A Job Manager, so that multiple job managers can be used.
    """

    def __init__(self, workflow):
        from cosmos import session
        self.local_jm = DRM_Local(workflow, self)
        if session.settings['DRM'] == 'Native_LSF':
            self.default_jm = JobManager_LSF(workflow, self)
        elif session.settings['DRM'] == 'local':
            self.default_jm = DRM_Local(workflow, self)
        else:
            self.default_jm = DRM_Local(workflow, self)
        self.workflow = workflow

    @property
    def jobAttempts(self):
        "This JobManager's jobAttempts"
        return JobAttempt.objects.filter(task__stage__workflow=self.workflow)

    def get_numJobsQueued(self):
        "The number of queued jobs."
        return self.jobAttempts.filter(queue_status = 'queued').count()

    def __create_command_sh(self, jobAttempt):
        """Create a sh script that will execute a command"""
        with open(jobAttempt.command_script_path,'wb') as f:
            f.write("#!/bin/bash\n")
            f.write(jobAttempt.command)
        os.system('chmod 700 {0}'.format(jobAttempt.command_script_path))

    def _create_cmd_str(self,jobAttempt):
        from cosmos import session
        "The command to be stored in the command.sh script"
        return "python {profile} -f {profile_out} {command_script_path}".format(
            profile = os.path.join(session.settings['cosmos_library_path'],'contrib/profile/profile.py'),
            db = jobAttempt.profile_output_path+'.sqlite',
            profile_out = jobAttempt.profile_output_path,
            command_script_path = jobAttempt.command_script_path
        )

    def add_jobAttempt(self, task, command, jobName = "Generic_Job_Name"):
        """
        Adds a new JobAttempt
        :param command: The system command to run
        :param jobName: an optional name for the jobAttempt
        :param jobinfo_output_dir: the directory to story the stdout and stderr files
        """
        jobAttempt = JobAttempt(jobManager=self, task=task, command = command, jobName = jobName)
        jobAttempt.command_script_path = os.path.join(jobAttempt.jobinfo_output_dir,'command.sh')
        check_and_create_output_dir(jobAttempt.jobinfo_output_dir)
        self.__create_command_sh(jobAttempt)
        jobAttempt.save()
        return jobAttempt


    def get_jobAttempt_status(self,jobAttempt):
        """
        Queries the DRM for the status of the job
        """
        if jobAttempt.task.always_local:
            self.local_jm.status(jobAttempt)
        else:
            self.default_jm.status(jobAttempt)


    def yield_all_queued_jobs(self):
        "Yield all queued jobs."
        i=0
        while self.get_numJobsQueued() > 0:
            i+=1
            sys.stderr.write(spinning_cursor(i))

            job = self._check_for_finished_job()

            sys.stderr.write('\b')

            if job != None:
                yield job
            else:
                time.sleep(1) #dont sleep if a job just returned

    def _check_for_finished_job(self):
        """
        Checks to see if one of the queued jobAttempts are finished.  If a jobAttempt is finished, this method
        is responsible for calling:
        jobAttempt._hasFinished()

        :return: A finished jobAttempt, or None if all queued jobs are still running
        """
        for jobAttempt in self.jobAttempts.filter(queue_status='queued'):
            if jobAttempt.task.always_local:
                exit_code = self.local_jm.poll(jobAttempt)
            else:
                exit_code = self.default_jm.poll(jobAttempt)

            if exit_code is not None:
                jobAttempt._hasFinished(exit_code == 0, {'exit_code': exit_code})
                return jobAttempt

    def get_status(self,jobAttempt):
        if jobAttempt.task.always_local:
            self.local_jm.status(jobAttempt)
        else:
            self.default_jm.status(jobAttempt)

    def terminate(self, jobAttempt):
        if jobAttempt.task.always_local:
            self.local_jm.terminate(jobAttempt)
        else:
            self.default_jm.terminate(jobAttempt)


    def submit_job(self,jobAttempt):
        """Submits and runs a job"""
        if not jobAttempt.queue_status == 'not_queued':
            raise JobStatusError, 'JobAttempt has already been submitted'

        if jobAttempt.task.always_local:
            self.local_jm.submit_job(jobAttempt)
        else:
            self.default_jm.submit_job(jobAttempt)

        jobAttempt.queue_status = 'queued'
        jobAttempt.save()
        return jobAttempt
