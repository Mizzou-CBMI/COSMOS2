import contextlib
import subprocess as sp
import json
import re
import os
from collections import OrderedDict
import tempfile
import time
from cosmos.job.drm.util import div, convert_size_to_kb, exit_process_group
from cosmos.util.signal_handlers import sleep_through_signals
from cosmos import TaskStatus

from more_itertools import grouper
from .DRM_Base import DRM


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
            out = sp.check_output(sub, env=os.environ, preexec_fn=exit_process_group, shell=True)
            task.drm_jobID = unicode(re.search('job (\d+)', out).group(1))
        except sp.CalledProcessError as cpe:
            task.log.error('%s submission to %s failed with error %s: %s' %
                           (task, task.drm, cpe.returncode, cpe.output.strip()))
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
        if len(tasks):
            qjobs = _qstat_all()

        for task in tasks:
            jid = unicode(task.drm_jobID)
            if jid not in qjobs or qjobs[jid]['STATE'] == 'COMPLETED':
                # if job does not appear in squeue or has "COMPLETED" status assume it's done with exit code 0 (unless
                # scontrol show jobid says otherwise)
                data = self._get_task_return_data(task)
                data['exit_status'] = (0 if data['exit_status'] is None else data['exit_status'])
                yield task, data
            elif any(finished_state in qjobs[jid]['STATE'] for finished_state in
                   ['BOOT_FAIL', 'CANCELLED', 'FAILED', 'NODE_FAIL', 'PREEMPTED', 'REVOKED', 'TIMEOUT']):
                # If the job is tagged with any of the "failure" exit codes, it has completed with exit code > 0
                data = self._get_task_return_data(task)
                data['exit_status'] = (1 if (data['exit_status'] is None or data['exit_status'] == 0)
                                       else data['exit_status'])
                yield task, data


    def drm_statuses(self, tasks):
        """
        :param tasks: tasks that have been submitted to the job manager
        :returns: (dict) task.drm_jobID -> drm_status
        """
        if len(tasks):
            qjobs = _qstat_all()

            def f(task):
                return qjobs.get(unicode(task.drm_jobID), dict()).get('STATE', '???')

            return {task.drm_jobID: f(task) for task in tasks}
        else:
            return {}


    def _get_task_return_data(self, task):
        """
        Convert raw qacct job data into Cosmos's more portable format.
        Returns a dictionary of job metadata
        """
        d = _qacct_raw(task)
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
        d['exit_status'] = exit_code
        task.workflow.log.info("%s returned with exit code: '%s'" % (task, str(exit_code)))
        return d


    def kill(self, task):
        "Terminates a task"
        raise NotImplementedError


    def kill_tasks(self, tasks):
        for group in grouper(50, tasks):
            group = filter(lambda x: x is not None, group)
            pids = map(lambda t: unicode(t.drm_jobID), group)
            sp.call(['scancel', '-Q'] + pids, preexec_fn=exit_process_group)


def _qacct_raw(task, timeout=600, quantum=15):
    """
    Parse "scontrol show jobid" output into key/value pairs.
    """
    start = time.time()
    num_retries = timeout / quantum

    for i in xrange(num_retries):
        qacct_returncode = 0
        with contextlib.closing(tempfile.TemporaryFile()) as qacct_stderr_fd:
            try:
                qacct_stdout_str = sp.check_output(['scontrol', 'show', 'jobid', '-d', '-o', unicode(task.drm_jobID)],
                                                   preexec_fn=exit_process_group, stderr=qacct_stderr_fd)
                if len(qacct_stdout_str.strip()):
                    break
            except sp.CalledProcessError as err:
                qacct_stdout_str = err.output.strip()
                qacct_returncode = err.returncode

            qacct_stderr_fd.seek(0)
            qacct_stderr_str = qacct_stderr_fd.read().strip()

            if 'slurm_load_jobs error: Invalid job id specified' == qacct_stderr_str:
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
        k, v = kv[:eq_pos], kv[(eq_pos+1):]
        acct_dict[k] = v

    return acct_dict


def _qstat_all():
    """
    returns a dict keyed by lsf job ids, who's values are a dict of bjob
    information about the job
    """
    try:
        lines = sp.check_output(['squeue', '-l'], preexec_fn=exit_process_group).strip().split('\n')
    except (sp.CalledProcessError, OSError):
        return {}
    keys = re.split("\s+", lines[1].strip())
    bjobs = {}
    for l in lines[2:]:
        items = re.split("\s+", l.strip())
        bjobs[items[0]] = dict(zip(keys, items))
    return bjobs


