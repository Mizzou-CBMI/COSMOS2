import os
import sys

from cosmos import session
from django.utils.datastructures import SortedDict
from cosmos.utils.helpers import enable_stderr,disable_stderr
from cosmos.config import settings
from cosmos.models.job.JobAttempt import JobAttempt


class JobStatusError(Exception):
    pass
class DRMAA_Error(Exception):pass

#######################
# Initialize DRMAA
#######################

os.environ['DRMAA_LIBRARY_PATH'] = settings['drmaa_library_path']
if settings['DRM'] == 'LSF':
    os.environ['LSF_DRMAA_CONF'] = os.path.join(settings['cosmos_library_path'],'lsf_drmaa.conf')

import drmaa

if session.settings['DRM'] != 'local':
    drmaa_enabled = False
    try:
        drmaa_session = drmaa.Session()
        drmaa_session.initialize()
        drmaa_enabled = True
    except Exception as e:
        print >> sys.stderr, "ERROR! Could not enable drmaa."
        raise


decode_drmaa_state = SortedDict([
    (drmaa.JobState.UNDETERMINED, 'process status cannot be determined'),
    (drmaa.JobState.QUEUED_ACTIVE, 'job is queued and active'),
    (drmaa.JobState.SYSTEM_ON_HOLD, 'job is queued and in system hold'),
    (drmaa.JobState.USER_ON_HOLD, 'job is queued and in user hold'),
    (drmaa.JobState.USER_SYSTEM_ON_HOLD, 'job is queued and in user and system hold'),
    (drmaa.JobState.RUNNING, 'job is running'),
    (drmaa.JobState.SYSTEM_SUSPENDED, 'job is system suspended'),
    (drmaa.JobState.USER_SUSPENDED, 'job is user suspended'),
    (drmaa.JobState.DONE, 'job finished normally'),
    (drmaa.JobState.FAILED, 'job finished, but failed'),
    ])

class JobManager(object):
    """
    Note there can only be one of these instantiated at a time
    """
    class Meta:
        app_label = 'cosmos'
        db_table = 'cosmos_jobmanager'

    def get_jobAttempt_status(self,jobAttempt):
        """
        Queries the DRM for the status of the job
        """
        raise NotImplementedError

    def terminate_jobAttempt(self,jobAttempt):
        "Terminates a jobAttempt"
        try:
            drmaa_session.control(str(jobAttempt.drmaa_jobID), drmaa.JobControlAction.TERMINATE)
            return True
        except drmaa.errors.InternalException:
            False

    def get_jobAttempt_status(self,jobAttempt):
        """
        Queries the DRM for the status of the job
        """
        try:
            s = decode_drmaa_state[drmaa_session.jobStatus(str(jobAttempt.drmaa_jobID))]
        except drmaa.InvalidJobException:
            if jobAttempt.queue_status == 'completed':
                if jobAttempt.successful:
                    s = decode_drmaa_state[drmaa.JobState.DONE]
                else:
                    s = decode_drmaa_state[drmaa.JobState.FAILED]
            else:
                s = 'not sure' #job doesnt exist in queue anymore but didn't succeed or fail
        return s

    def __drmaa_createJobTemplate(self,jobAttempt):
        """
        Creates a JobTemplate for jobAttempt

        Possible attrs are:
        ['args','blockEmail','deadlineTime','delete','email','errorPath','hardRunDurationLimit'
        'hardWallclockTimeLimit','inputPath','jobCategory','jobEnvironment','jobName','jobSubmissionState',
        'joinFiles','nativeSpecification','outputPath','remoteCommand','softRunDurationLimit','softWallclockTimeLimit',
        'startTime','transferFiles','workingDirectory','cpu_time']

        """
        cmd = self._create_cmd_str(jobAttempt)

        jt = drmaa_session.createJobTemplate()
        jt.workingDirectory = session.settings['working_directory']
        #jt.remoteCommand = self.command_script_path
        #jt.args = self.command_script_text.split(' ')[1:]
        jt.remoteCommand = cmd.split(' ')[0]
        jt.args = cmd.split(' ')[1:]
        #jt.workingDirectory = os.getcwd()
        jt.jobName = jobAttempt.task.stage.name
        jt.outputPath = ':'+jobAttempt.STDOUT_filepath
        jt.errorPath = ':'+jobAttempt.STDERR_filepath

        jt.jobEnvironment = os.environ
        jt.nativeSpecification = session.get_drmaa_native_specification(jobAttempt)

        return jt

    def _submit_job(self,jobAttempt):
        jobTemplate = self.__drmaa_createJobTemplate(jobAttempt)
        jobAttempt.drmaa_jobID = drmaa_session.runJob(jobTemplate)
        jobAttempt.drmaa_native_specification = jobTemplate.nativeSpecification
        jobTemplate.delete() #prevents memory leak


    def _check_for_finished_job(self):
        """
        Waits for any job to finish, and returns that JobAttempt.  If there are no jobs left, returns None.
        All the enable/disable stderr stuff is because LSF drmaa prints really annoying messages that mean nothing.
        """
        extra_jobinfo = None
        try:
            disable_stderr() #python drmaa prints whacky messages sometimes.  if the script just quits without printing anything, something really bad happend while stderr is disabled
            extra_jobinfo = drmaa_session.wait(jobId=drmaa.Session.JOB_IDS_SESSION_ANY,timeout=drmaa.Session.TIMEOUT_NO_WAIT)
            enable_stderr()
        except drmaa.errors.InvalidJobException as e:
            # There are no jobs to wait on.
            # This should never happen since I check for num_queued_jobs in yield_all_queued_jobs
            enable_stderr()
            raise DRMAA_Error('drmaa_session.wait threw invalid job exception.  there are no jobs left.  make sure jobs are queued before calling _check_for_finished_job.')
        except drmaa.errors.ExitTimeoutException:
            #jobs are queued, but none are done yet
            enable_stderr()
            return None
        except Exception as e:
            enable_stderr()
            print e
        finally:
            enable_stderr()

        if extra_jobinfo is None:
            raise DRMAA_Error, 'DRMAA did not return the job information.  This has been reported to happen with LSF and ' \
                               'the only known fix is to try again, which usually works.'

        jobAttempt = JobAttempt.objects.get(drmaa_jobID = extra_jobinfo.jobId)

        extra_jobinfo = extra_jobinfo._asdict()

        successful = extra_jobinfo is not None and extra_jobinfo['exitStatus'] == 0 and extra_jobinfo['wasAborted'] == False and extra_jobinfo['hasExited']
        jobAttempt._hasFinished(successful, extra_jobinfo)
        return jobAttempt

