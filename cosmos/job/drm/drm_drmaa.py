from .DRM_Base import DRM

import os
from cosmos.api import only_one


class DRM_DRMAA(DRM):
    name = 'drmaa'

    def __init__(self, *args, **kwargs):
        super(DRM_DRMAA, self).__init__(*args, **kwargs)

    @property
    def session(self):
        if self._session is None:
            import drmaa
            self._session = drmaa.Session()
        else:
            return self._session



    def submit_job(self, task):

        jt = self.session.createJobTemplate()
        # jt.workingDirectory = settings['working_directory']
        jt.remoteCommand = task.output_command_script_path
        # jt.args             = cmd.split(' ')[1:]
        # jt.jobName          = jobAttempt.task.stage.name
        jt.outputPath = ':' + task.output_stdout_path
        jt.errorPath = ':' + task.output_stderr_path
        jt.jobEnvironment = os.environ

        jt.nativeSpecification = task.drm_native_specification or ''

        drm_jobID = self.session.runJob(jt)

        # prevents memory leak
        self.session.deleteJobTemplate(jt)

        return drm_jobID

    def filter_is_done(self, tasks):
        import drmaa
        # Keep yielding jobs until timeout > 1s occurs or there are no jobs
        while True:
            try:
                # disable_stderr() #python drmaa prints whacky messages sometimes.  if the script just quits without printing anything, something really bad happend while stderr is disabled
                extra_jobinfo = self.session.wait(jobId=drmaa.Session.JOB_IDS_SESSION_ANY, timeout=1)
                # enable_stderr()
            except drmaa.errors.InvalidJobException as e:
                # There are no jobs to wait on.
                # This should never happen since I check for num_queued_jobs in yield_all_queued_jobs
                # enable_stderr()
                break
                # raise AssertionError('This should never happen since I check for num_queued_jobs in yield_all_queued_jobs')
            except drmaa.errors.ExitTimeoutException:
                # jobs are queued, but none are done yet
                # enable_stderr()
                break

            extra_jobinfo = extra_jobinfo._asdict()
            extra_jobinfo['successful'] = extra_jobinfo is not None and extra_jobinfo['exitStatus'] == 0 and extra_jobinfo['wasAborted'] == False and \
                                          extra_jobinfo['hasExited']
            yield only_one(t for t in tasks if t.drm_jobID == extra_jobinfo['jobId']), extra_jobinfo

    def drm_statuses(self, tasks):
        return {task.drm_jobID: self.decodestatus[self.session.jobStatus(str(task.drm_jobID))] for task in tasks}

    def kill(self, task):
        "Terminates a task"
        import drmaa
        self.session.control(str(task.drmaa_jobID), drmaa.JobControlAction.TERMINATE)

    def kill_tasks(self, tasks):
        for t in tasks:
            self.kill(t)

    @property
    def decodestatus(self):
        import drmaa
        return {drmaa.JobState.UNDETERMINED: 'process status cannot be determined',
                drmaa.JobState.QUEUED_ACTIVE: 'job is queued and active',
                drmaa.JobState.SYSTEM_ON_HOLD: 'job is queued and in system hold',
                drmaa.JobState.USER_ON_HOLD: 'job is queued and in user hold',
                drmaa.JobState.USER_SYSTEM_ON_HOLD: 'job is queued and in user and system hold',
                drmaa.JobState.RUNNING: 'job is running',
                drmaa.JobState.SYSTEM_SUSPENDED: 'job is system suspended',
                drmaa.JobState.USER_SUSPENDED: 'job is user suspended',
                drmaa.JobState.DONE: 'job finished normally',
                drmaa.JobState.FAILED: 'job finished, but failed'}
