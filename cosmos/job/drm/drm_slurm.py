import datetime
import os
import re
import subprocess as sp
from pprint import pformat

from more_itertools import grouper
from cosmos.util.retry import retry_call

from cosmos import TaskStatus
from cosmos.job.drm.DRM_Base import DRM
from cosmos.job.drm.util import exit_process_group, convert_size_to_kb, div, check_output_and_stderr

FAILED_STATES = ['BOOT_FAIL', 'CANCELLED', 'FAILED', 'PREEMPTED', 'REVOKED', 'TIMEOUT', 'CANCELLED by 0']
PENDING_STATES = ['PENDING', 'CONFIGURING', 'COMPLETING', 'RUNNING', 'NODE_FAIL', 'RESIZING', 'SUSPENDED']
COMPLETED_STATES = ['COMPLETED', ]


def parse_slurm_time(s, default=0):
    """
    >>> parse_slurm_time('03:53:03') / 60 / 60
    3.8841666666666668
    >>> parse_slurm_time('24-02:40:+') / 60 / 60
    578.6666666666666
    >>> parse_slurm_time('06:20:01') / 60 / 60
    6.333611111111111
    >>> parse_slurm_time('2-03:19:54') / 60 / 60
    51.33166666666667
    """

    if s.strip() == '':
        return default

    p = s.split('-')
    if len(p) == 2:
        days = p[0]
        time = p[1]
    elif len(p) == 1:
        days = 0
        time = p[0]
    else:
        raise AssertionError('impossible')

    hours, mins, secs = time.split(':')
    if secs == '+':
        secs = 0
    return int(days) * 24 * 60 * 60 + int(hours) * 60 * 60 + int(mins) * 60 + int(secs)


def parse_slurm_date(s):
    return datetime.datetime.strptime(s, "%Y-%m-%dT%H:%M:%S")


def sbatch(task):
    ns = task.drm_native_specification if task.drm_native_specification else ''

    cmd = (['sbatch', '-o', os.path.abspath(task.output_stdout_path), '-e', os.path.abspath(task.output_stderr_path)]
           + ns.split()
           + [task.output_command_script_path])

    out, err = check_output_and_stderr(cmd, env=os.environ, preexec_fn=exit_process_group)
    return str(re.search(r'job (\d+)', out).group(1))


class DRM_SLURM(DRM):
    name = 'slurm'
    poll_interval = 5

    def submit_job(self, task):
        for p in [task.output_stdout_path, task.output_stderr_path]:
            if os.path.exists(p):
                os.unlink(p)

        task.drm_jobID = retry_call(sbatch, fargs=[task],
                                    delay=10, tries=10, backoff=2, max_delay=60,
                                    logger=task.log)
        task.status = TaskStatus.submitted

    def filter_is_done(self, tasks):
        """
        Yield a dictionary of Slurm job metadata for each task that has completed.
        """
        # jobid can be none if submission fialed
        job_ids = [t.drm_jobID for t in tasks if t.drm_jobID is not None]
        if job_ids:
            job_infos = retry_call(do_sacct, fargs=[job_ids],
                                   delay=10, tries=10, backoff=2, max_delay=60,
                                   logger=tasks[0].workflow.log)

            for task in tasks:
                if task.drm_jobID in job_infos:
                    job_info = job_infos[task.drm_jobID]
                    if job_info['State'] in FAILED_STATES + COMPLETED_STATES:
                        job_info = parse_sacct(job_infos[task.drm_jobID],
                                               tasks[0].workflow.log)

                        yield task, job_info
                    else:
                        assert job_info['State'] in PENDING_STATES, 'Invalid job state: `%s` for %s drm_job_id=%s' % (job_info['State'], task, task.drm_jobID)

    def drm_statuses(self, tasks, log_errors=True):
        """
        :param tasks: tasks that have been submitted to the job manager
        :returns: (dict) task.drm_jobID -> drm_status
        """
        job_ids = [t.drm_jobID for t in tasks if t.drm_jobID is not None]
        if job_ids:
            job_infos = retry_call(do_sacct, fargs=[job_ids],
                                   delay=10, tries=10, backoff=2, max_delay=60,
                                   logger=tasks[0].workflow.log)

            def f(task):
                return job_infos.get(task.drm_jobID, dict()).get('State', 'UNK_JOB_STATE')

            return {task.drm_jobID: f(task) for task in tasks}
        else:
            return {}

    def kill(self, task):
        """Terminate a task."""
        raise NotImplementedError

    def kill_tasks(self, tasks):
        for group in grouper(50, tasks):
            group = filter(lambda x: x is not None, group)
            pids = map(lambda t: unicode(t.drm_jobID), group)
            sp.call(['scancel', '-Q'] + pids, preexec_fn=exit_process_group)


def do_sacct(job_ids):
    # there's a lag between when a job finishes and when sacct is available :(Z
    cmd = 'sacct --format=' \
          '"State,JobID,CPUTime,MaxRSS,AveRSS,AveCPU,CPUTimeRAW,AveVMSize,MaxVMSize,Elapsed,ExitCode,Start,End" ' \
          '-j %s -P' % ','.join(job_ids)

    out, err = check_output_and_stderr(cmd,
                                       preexec_fn=exit_process_group,
                                       shell=True
                                       )

    parts = out.strip().split("\n")
    # job_id_to_job_info_dict
    all_jobs = dict()
    # first line is the header
    keys = parts[0].split('|')
    # second line is all dashes, ignore it
    for line in parts[2:]:
        values = line.split('|')
        job_dict = dict(zip(keys, values))

        if 'batch' in job_dict['JobID']:
            # slurm prints these .batch versions of jobids which have better information, overwrite
            job_dict['JobID'] = job_dict['JobID'].replace('.batch', '')

        all_jobs[job_dict['JobID']] = job_dict

    return all_jobs


def parse_sacct(job_info, log=None):
    try:
        job_info2 = job_info.copy()
        if job_info2['State'] in FAILED_STATES + PENDING_STATES:
            job_info2['exit_status'] = None
        else:
            job_info2['exit_status'] = int(job_info2['ExitCode'].split(":")[0])
        job_info2['cpu_time'] = int(job_info2['CPUTimeRAW'])
        job_info2['wall_time'] = parse_slurm_time(job_info2['Elapsed'])
        job_info2['percent_cpu'] = div(float(job_info2['cpu_time']), float(job_info2['wall_time']))

        job_info2['avg_rss_mem'] = convert_size_to_kb(job_info2['AveRSS']) if job_info2['AveRSS'] != '' else None
        job_info2['max_rss_mem'] = convert_size_to_kb(job_info2['MaxRSS']) if job_info2['MaxRSS'] != ''  else None
        job_info2['avg_vms_mem'] = convert_size_to_kb(job_info2['AveVMSize']) if job_info2['AveVMSize'] != '' else None
        job_info2['max_vms_mem'] = convert_size_to_kb(job_info2['MaxVMSize']) if job_info2['MaxVMSize'] != '' else None
    except Exception as e:
        if log:
            log.info('Error Parsing: %s' % pformat(job_info2))
        raise e

    return job_info2
