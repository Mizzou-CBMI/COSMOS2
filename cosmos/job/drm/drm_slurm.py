import datetime
import json
import os
import re
import subprocess as sp
import time

from more_itertools import grouper

from cosmos import TaskStatus
from cosmos.job.drm.DRM_Base import DRM
from cosmos.job.drm.util import exit_process_group, CosmosCalledProcessError, check_output_and_stderr
from cosmos.util.signal_handlers import sleep_through_signals

FAILED_STATES = ['BOOT_FAIL', 'CANCELLED', 'FAILED', 'NODE_FAIL', 'PREEMPTED', 'REVOKED', 'TIMEOUT']


def parse_slurm_time(s, default=0):
    if s.strip() == '':
        return default

    p = s.split('-')
    if len(p) == 2:
        days = p[0]
        time = p[1]
    else:
        days = 0
        time = p[0]
    hours, mins, secs = time.split(':')
    return int(days) * 24 * 60 * 60 + int(hours) * 60 * 60 + int(mins) * 60 + int(secs)


def parse_slurm_time2(s):
    return datetime.datetime.strptime(s, "%Y-%m-%dT%H:%M:%S")


class DRM_SLURM(DRM):
    name = 'slurm'
    poll_interval = 5

    def submit_job(self, task):
        for p in [task.output_stdout_path, task.output_stderr_path]:
            if os.path.exists(p):
                os.unlink(p)

        ns = ' ' + task.drm_native_specification if task.drm_native_specification else ''
        sub = "sbatch -o {stdout} -e {stderr} {ns} {cmd_str}".format(
            stdout=task.output_stdout_path,
            stderr=task.output_stderr_path,
            ns=ns,
            cmd_str=task.output_command_script_path)

        try:
            out = sp.check_output(sub, env=os.environ, preexec_fn=exit_process_group, shell=True).decode()
            task.drm_jobID = unicode(re.search(r'job (\d+)', out).group(1))
        except sp.CalledProcessError as cpe:
            task.log.error('%s submission to %s failed with error %s: %s' %
                           (task, task.drm, cpe.returncode, cpe.output.decode().strip()))
            task.status = TaskStatus.failed
        except ValueError:
            task.log.error('%s submission to %s returned unexpected text: %s' % (task, task.drm, out))
            task.status = TaskStatus.failed
        else:
            task.status = TaskStatus.submitted

    def filter_is_done(self, tasks):
        """
        Yield a dictionary of Slurm job metadata for each task that has completed.
        """
        if tasks:
            qjobs = _qstat_all(tasks[0].workflow.log)

        for task in tasks:
            jid = unicode(task.drm_jobID)

            if jid not in qjobs or qjobs[jid]['STATE'] == 'COMPLETED' or qjobs[jid]['STATE'] in FAILED_STATES:
                # job is done
                data = self._get_task_return_data(task)

                if data['JobState'] in FAILED_STATES:
                    data['exit_status'] = (1 if (data['exit_status'] is None or data['exit_status'] == 0)
                                           else data['exit_status'])
                else:
                    data['exit_status'] = (0 if data['exit_status'] is None else data['exit_status'])

                yield task, data

    def drm_statuses(self, tasks, log_errors=True):
        """
        :param tasks: tasks that have been submitted to the job manager
        :returns: (dict) task.drm_jobID -> drm_status
        """
        if tasks:
            qjobs = _qstat_all(log=tasks[0].workflow.log if log_errors else None)

            def f(task):
                return qjobs.get(unicode(task.drm_jobID), dict()).get('STATE', 'UNK_JOB_STATE')

            return {task.drm_jobID: f(task) for task in tasks}
        else:
            return {}

    def _get_task_return_data(self, task):
        """
        Convert raw qacct job data into Cosmos's more portable format.
        Returns a dictionary of job metadata
        """
        d = _scontrol_raw(task)
        job_state = d.get("JobState", 'COMPLETED')
        if job_state != 'COMPLETED':
            task.workflow.log.warn('%s Slurm (scontrol show jobid -d -o %s) reports JobState %s:\n%s' %
                                   (task, task.drm_jobID, job_state,
                                    json.dumps(d, indent=4, sort_keys=True)))
        if 'DerivedExitCode' in d and 'ExitCode' in d:
            exit_code = (0 if d['DerivedExitCode'] == '0:0' and d['ExitCode'] == '0:0'
                         else max(max(int(c) for c in d['DerivedExitCode'].split(":")),
                                  max(int(c) for c in d['ExitCode'].split(":"))))
        else:
            # scontrol show jobid -d -o did not find the job id (probably called too late) so we don't have exit code
            exit_code = None

        # there's a delay before sacct info is available.  To keep things fast should we just update all the jobs
        # at the end of workflow.run?
        # d2 = get_resource_usage(task.drm_jobID)

        d['exit_status'] = exit_code
        d['wall_time'] = (parse_slurm_time2(d['EndTime']) - parse_slurm_time2(d['StartTime'])).total_seconds()
        # d['cpu_time'] = parse_slurm_time(d2['AveCPU'])
        # d['percent_cpu'] = div(float(d['cpu_time']), float(d['wall_time']))
        # d['avg_rss_mem'] = convert_size_to_kb(d2['AveRSS'])
        # d['avg_vms_mem'] = convert_size_to_kb(d2['AveVMSize'])
        # task.workflow.log.info("%s returned with exit code: '%s'" % (task, str(exit_code)))
        return d

    def kill(self, task):
        """Terminate a task."""
        raise NotImplementedError

    def kill_tasks(self, tasks):
        for group in grouper(50, tasks):
            group = filter(lambda x: x is not None, group)
            pids = map(lambda t: unicode(t.drm_jobID), group)
            sp.call(['scancel', '-Q'] + pids, preexec_fn=exit_process_group)


def _scontrol_raw(task, timeout=600, quantum=15):
    """
    Parse "scontrol show jobid" output into key/value pairs.
    """
    start = time.time()
    num_retries = int(timeout / quantum)

    for i in xrange(num_retries):
        qacct_returncode = 0
        try:
            qacct_stdout_str, qacct_stderr_str = check_output_and_stderr(
                ['scontrol', 'show', 'jobid', '-d', '-o', unicode(task.drm_jobID)],
                preexec_fn=exit_process_group)
            if qacct_stdout_str.strip():
                break
        except CosmosCalledProcessError as err:
            qacct_stdout_str = err.output.strip()
            qacct_stderr_str = err.stderr.strip()
            qacct_returncode = err.returncode

            if qacct_stderr_str == 'slurm_load_jobs error: Invalid job id specified':
                # too many jobs were scheduled since it finished and the job id was forgotten
                return dict(JobId=task.drm_jobID)
            else:
                task.workflow.log.error('%s Slurm (scontrol show jobid -d -o %s) returned error code %d',
                                        task, task.drm_jobID, qacct_returncode)
                if qacct_stdout_str or qacct_stderr_str:
                    task.workflow.log.error('%s Slurm (scontrol show jobid -d -o %s) printed the following',
                                            task, task.drm_jobID)
                    if qacct_stdout_str:
                        task.workflow.log.error('stdout: "%s"', qacct_stdout_str)
                    if qacct_stderr_str:
                        task.workflow.log.error('stderr: "%s"', qacct_stderr_str)

        if i > 0:
            task.workflow.log.info(
                '%s Slurm (scontrol show jobid -d -o %s) attempt %d failed %d sec after first attempt%s',
                task, task.drm_jobID, i + 1, time.time() - start,
                '. Will recheck job status after %d sec' % quantum if i + 1 < num_retries else '')
        if i + 1 < num_retries:
            sleep_through_signals(timeout=quantum)
    else:
        # fallthrough: all retries failed
        raise ValueError('No valid `scontrol show jobid -d -o %s` output after %d tries and %d sec' %
                         (task.drm_jobID, i, time.time() - start))

    acct_dict = {}
    k, v = None, None
    for kv in qacct_stdout_str.strip().split():
        eq_pos = kv.find('=')
        if eq_pos == -1:
            # add the string to previous value - most likely the previous value contained a white space
            if k is not None:
                acct_dict[k] += (" " + kv)
                continue
            else:
                raise EnvironmentError('%s with drm_jobID=%s has unparseable "scontrol show jobid -d -o" output:\n%s\n'
                                       'Could not find "=" in "%s"' %
                                       (task, task.drm_jobID, qacct_stdout_str, kv))
        k, v = kv[:eq_pos], kv[(eq_pos + 1):]
        acct_dict[k] = v

    return acct_dict


def _qstat_all(log=None, timeout=60 * 10):
    """
    returns a dict keyed by lsf job ids, who's values are a dict of bjob
    information about the job
    """
    start = time.time()
    while time.time() - start < timeout:
        try:
            lines = sp.check_output(['squeue', '-l'], preexec_fn=exit_process_group).decode().strip().split('\n')
            break
        except (sp.CalledProcessError, OSError) as e:
            # sometimes slurm goes quiet
            if log:
                log.info('Error running squeue: %s' % e)
        time.sleep(10)
    else:
        return {}

    keys = re.split(r"\s+", lines[1].strip())
    bjobs = {}
    for l in lines[2:]:
        items = re.split(r"\s+", l.strip())
        bjobs[items[0]] = dict(zip(keys, items))
    return bjobs


def get_resource_usage(job_id):
    # there's a lag between when a job finishes and when sacct is available :(Z
    parts = sp.check_output('sacct --format="CPUTime,MaxRSS,AveRSS,AveCPU,CPUTimeRAW,Elapsed" -j %s' % job_id,
                            shell=True).decode().strip().split("\n")
    keys = parts[0].split()
    values = parts[-1].split()
    return dict(zip(keys, values))
