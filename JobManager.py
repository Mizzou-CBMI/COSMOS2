import os,sys
from .JobAttempt import JobAttempt
from cosmos.utils.helpers import check_and_create_output_dir
from cosmos.utils.helpers import spinning_cursor
import time

class JobStatusError(Exception):
    pass

class JobManager():
    @property
    def jobAttempts(self):
        "This JobManager's jobAttempts"
        return JobAttempt.objects.filter(jobManager=self)


    def get_numJobsQueued(self):
        "The number of queued jobs."
        return self.jobAttempts.filter(queue_status = 'queued').count()

    def __create_command_sh(self,jobAttempt):
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

    def get_jobAttempt_status(self,jobAttempt):
        """
        Queries the DRM for the status of the job
        """
        raise NotImplementedError

    def terminate_jobAttempt(self,jobAttempt):
        "Terminates a jobAttempt"
        raise NotImplementedError

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
        raise NotImplementedError

    def submit_job(self,jobAttempt):
        """Submits and runs a job"""
        if not jobAttempt.queue_status == 'not_queued':
            raise JobStatusError, 'JobAttempt has already been submitted'

        self._submit_job(jobAttempt)

        jobAttempt.queue_status = 'queued'
        jobAttempt.save()
        return jobAttempt

    def _submit_job(self,jobAttempt):
        "Submit the jobAttempt to the DRM"
        raise NotImplementedError


    def toString(self):
        return "JobManager %s, created on %s" % (self.id,self.created_on)