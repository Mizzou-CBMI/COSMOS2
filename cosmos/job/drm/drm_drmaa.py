import os
import sys

from .DRM_Base import DRM
from .util import div, convert_size_to_kb

_drmaa_session = None

def get_drmaa_session():
    global _drmaa_session
    if _drmaa_session is None:
        import drmaa
        _drmaa_session = drmaa.Session()
        _drmaa_session.initialize()
    return _drmaa_session

class DRM_DRMAA(DRM):
    name = 'drmaa'
    poll_interval = 5

    _session = None

    def __init__(self, *args, **kwargs):
        super(DRM_DRMAA, self).__init__(*args, **kwargs)

    def submit_job(self, task):
        with get_drmaa_session().createJobTemplate() as jt:
            jt.remoteCommand = os.path.abspath(task.output_command_script_path)
            jt.outputPath = ':' + os.path.abspath(task.output_stdout_path)
            jt.errorPath = ':' + os.path.abspath(task.output_stderr_path)
            jt.jobEnvironment = os.environ
            jt.nativeSpecification = task.drm_native_specification or ''

            try:
                drm_jobID = get_drmaa_session().runJob(jt)
            except BaseException:
                print >>sys.stderr, \
                    "Couldn't run %s with nativeSpecification=`%s`" % \
                    (task, jt.nativeSpecification)
                raise

        return drm_jobID

    def filter_is_done(self, tasks):
        import drmaa

        jobid_to_task = {t.drm_jobID: t for t in tasks}
        # Keep yielding jobs until timeout > 1s occurs or there are no jobs
        while len(jobid_to_task):

            try:
                # disable_stderr() #python drmaa prints whacky messages sometimes.  if the script just quits without printing anything, something really bad happend while stderr is disabled
                drmaa_jobinfo = get_drmaa_session().wait(jobId=drmaa.Session.JOB_IDS_SESSION_ANY, timeout=1)._asdict()
                # enable_stderr()

                yield jobid_to_task.pop(unicode(drmaa_jobinfo['jobId'])), \
                      parse_drmaa_jobinfo(drmaa_jobinfo)

            except drmaa.errors.ExitTimeoutException:
                # Jobs are queued, but none are done yet. Exit loop.
                # enable_stderr()
                break

            except drmaa.errors.InvalidJobException:
                # There are no jobs left to wait on!
                raise RuntimeError('Should not be waiting on non-existent jobs.')

            except Exception as exc:
                #
                # python-drmaa occasionally throws a naked Exception. Yuk!
                #
                # 'code 24' may occur when a running or queued job is killed.
                # If we see that, one or more jobs may be dead, but if so,
                # which one(s)? drmaa.Session.wait() hasn't returned a job id,
                # or much of anything.
                #
                # TODO This code correctly handles cases when a running job is
                # TODO killed, but killing a *queued* job (before it is even
                # TODO scheduled) can really foul things up, in ways I don't
                # TODO quite understand. We can find and flag the failed job,
                # TODO but subsequent calls to wait() either block indefinitely
                # TODO or throw a bunch of exceptions that kill the Cosmos
                # TODO process. Personally, I blame python-drmaa, but still, it
                # TODO would be nice to handle this error case more gracefully.
                #
                # TL;DR Don't kill queued jobs!!!
                #
                if not exc.message.startswith("code 24"):
                    # not sure we can handle other bare drmaa exceptions cleanly
                    raise

                # "code 24: no usage information was returned for the completed job"
                print >>sys.stderr, 'drmaa raised a naked Exception while ' \
                                    'fetching job status - an existing job may ' \
                                    'have been killed'
                #
                # Check the status of each outstanding job and fake
                # a failure status for any that have gone missing.
                #
                for jobid in jobid_to_task.keys():
                    try:
                        drmaa_jobstatus = get_drmaa_session().jobStatus(unicode(jobid))
                    except drmaa.errors.InvalidJobException:
                        drmaa_jobstatus = drmaa.JobState.UNDETERMINED
                    except Exception:
                        drmaa_jobstatus = drmaa.JobState.UNDETERMINED

                    if drmaa_jobstatus == drmaa.JobState.UNDETERMINED:
                        print >>sys.stderr, 'job %s is missing and presumed dead' % jobid
                        yield jobid_to_task.pop(jobid), \
                              create_empty_drmaa_jobinfo(os.EX_TEMPFAIL)

    def drm_statuses(self, tasks):
        import drmaa

        def get_status(task):
            try:
                return self.decodestatus[get_drmaa_session().jobStatus(unicode(task.drm_jobID))] if task.drm_jobID is not None else '?'
            except drmaa.errors.InvalidJobException:
                return '?'
            except:
                return '??'

        return {task.drm_jobID: get_status(task) for task in tasks}

    def kill(self, task):
        "Terminates a task"
        import drmaa

        if task.drm_jobID is not None:
            try:
                get_drmaa_session().control(unicode(task.drm_jobID), drmaa.JobControlAction.TERMINATE)
            except drmaa.errors.InvalidJobException:
                pass

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


def parse_drmaa_jobinfo(drmaa_jobinfo):
    d = drmaa_jobinfo['resourceUsage']
    cosmos_jobinfo = dict(
        exit_status=int(drmaa_jobinfo.get('exitStatus', os.EX_UNAVAILABLE)),

        percent_cpu=div(float(d.get('cpu', 0)),
                        float(d.get('ru_wallclock', 0))),
        wall_time=float(d.get('ru_wallclock', 0)),

        cpu_time=float(d.get('cpu', 0)),
        user_time=float(d.get('ru_utime', 0)),
        system_time=float(d.get('ru_stime', 0)),

        # TODO should we be calling convert_size_to_kb() for avg_rss_mem?
        avg_rss_mem=d.get('ru_ixrss', "0"),
        max_rss_mem_kb=convert_size_to_kb(d.get('ru_maxrss', "0")),
        avg_vms_mem_kb=None,
        max_vms_mem_kb=convert_size_to_kb(d.get('maxvmem', "0")),

        io_read_count=int(float(d.get('ru_inblock', 0))),
        io_write_count=int(float(d.get('ru_oublock', 0))),
        io_wait=float(d.get('iow', 0)),
        io_read_kb=float(d.get('io', 0)),
        io_write_kb=float(d.get('io', 0)),

        ctx_switch_voluntary=int(float(d.get('ru_nvcsw', 0))),
        ctx_switch_involuntary=int(float(d.get('ru_nivcsw', 0))),

        avg_num_threads=None,
        max_num_threads=None,

        avg_num_fds=None,
        max_num_fds=None,

        memory=float(d.get('mem', 0)),
    )

    #
    # Wait, what? drmaa has two exit status fields? Of course, they don't always
    # agree when an error occurs. Worse, sometimes drmaa doesn't set exit_status
    # when a job is killed. We may not be able to get the exact exit code, but
    # at least we can guarantee it will be non-zero for any job that shows signs
    # of terminating in error.
    #
    if int(drmaa_jobinfo['exitStatus']) != 0 or \
       drmaa_jobinfo['hasSignal'] or \
       drmaa_jobinfo['wasAborted'] or \
       not drmaa_jobinfo['hasExited']:

        if cosmos_jobinfo['exit_status'] == 0:
            try:
                cosmos_jobinfo['exit_status'] = int(float(
                    drmaa_jobinfo['resourceUsage']['exit_status']))
            except KeyError:
                cosmos_jobinfo['exit_status'] = os.EX_UNAVAILABLE

        if cosmos_jobinfo['exit_status'] == 0:
            cosmos_jobinfo['exit_status'] = os.EX_SOFTWARE

        cosmos_jobinfo['successful'] = False
    else:
        cosmos_jobinfo['successful'] = True

    return cosmos_jobinfo


def create_empty_drmaa_jobinfo(exit_status):

    return dict(
        exit_status=int(exit_status),
        successful=(int(exit_status) == 0),

        percent_cpu=0.0,
        wall_time=0.0,

        cpu_time=0.0,
        user_time=0.0,
        system_time=0.0,

        avg_rss_mem=0.0,
        max_rss_mem_kb=0.0,
        avg_vms_mem_kb=None,
        max_vms_mem_kb=0.0,

        io_read_count=0,
        io_write_count=0,
        io_wait=0.0,
        io_read_kb=0.0,
        io_write_kb=0.0,

        ctx_switch_voluntary=0,
        ctx_switch_involuntary=0,

        avg_num_threads=None,
        max_num_threads=None,
        avg_num_fds=None,
        max_num_fds=None,
        memory=0.0
    )
