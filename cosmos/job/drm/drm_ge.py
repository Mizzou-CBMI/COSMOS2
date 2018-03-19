import json
import os
import re
import subprocess
import time

from collections import OrderedDict

from more_itertools import grouper

from cosmos import TaskStatus
from cosmos.job.drm.DRM_Base import DRM
from cosmos.job.drm.util import (check_output_and_stderr, convert_size_to_kb, div,
                                 exit_process_group, DetailedCalledProcessError)
from cosmos.util.signal_handlers import sleep_through_signals


class DRM_GE(DRM):
    name = 'ge'
    poll_interval = 5

    def submit_job(self, task):
        for p in [task.output_stdout_path, task.output_stderr_path]:
            if os.path.exists(p):
                os.unlink(p)

        ns = ' ' + task.drm_native_specification if task.drm_native_specification else ''
        qsub = 'qsub -terse -o {stdout} -e {stderr} -b y -w e -cwd -S /bin/bash -V{ns} '.format(
            stdout=task.output_stdout_path, stderr=task.output_stderr_path, ns=ns)

        try:
            out = subprocess.check_output(
                '{qsub} "{cmd_str}"'.format(cmd_str=task.output_command_script_path, qsub=qsub),
                env=os.environ, preexec_fn=exit_process_group, shell=True, stderr=subprocess.STDOUT).decode()

            task.drm_jobID = unicode(int(out))
        except subprocess.CalledProcessError as cpe:
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
        Yield a dictionary of SGE job metadata for each task that has completed.

        This method tries to be defensive against corrupt qstat and qacct output.
        If qstat reports that a job has finished, but qacct output looks
        suspicious, we try to give the job, and/or SGE, time to complete and/or
        recover.

        This method will only yield corrupt qacct data if every outstanding task
        has been affected by this SGE bug.
        """
        if tasks:
            qjobs = _qstat_all()
            corrupt_data = {}

        for task in tasks:
            jid = unicode(task.drm_jobID)
            if jid not in qjobs or \
               any(finished_state in qjobs[jid]['state'] for finished_state in ['e', 'E']):
                #
                # If the job doesn't appear in qstat (or is tagged with 'e' or 'E'),
                # it *probably* has completed. However, SGE's qmaster may have
                # simply lost track of it for a little while, in which case qacct
                # will output corrupt data when it is interrogated.
                #
                data, data_are_corrupt = self._get_task_return_data(task)
                if data_are_corrupt:
                    task.workflow.log.warn(
                        '%s Corrupt SGE qstat/qacct output means it may still be running', task)
                    corrupt_data[task] = data
                else:
                    yield task, data

        num_cleanly_running_jobs = len(tasks) - len(corrupt_data)

        if num_cleanly_running_jobs > 0:
            for task in corrupt_data.keys():
                task.workflow.log.info(
                    '%s Temporarily masking corrupt SGE output since %d other jobs are running cleanly' %
                    (task, num_cleanly_running_jobs))
        else:
            for task, data in corrupt_data.items():
                task.workflow.log.error(
                    '%s All outstanding drm_ge tasks have corrupt SGE output: giving up on this one' % task)
                yield task, data

    def drm_statuses(self, tasks):
        """
        :param tasks: tasks that have been submitted to the job manager
        :returns: (dict) task.drm_jobID -> drm_status
        """
        if tasks:
            qjobs = _qstat_all()

            def f(task):
                return qjobs.get(unicode(task.drm_jobID), dict()).get('state', 'UNK_JOB_STATE')

            return {task.drm_jobID: f(task) for task in tasks}
        else:
            return {}

    def _get_task_return_data(self, task):
        """
        Convert raw qacct job data into Cosmos's more portable format.

        Returns a 2-tuple comprising:
        [0] a dictionary of job metadata,
        [1] a boolean indicating whether the metadata in [0] are affected by an
            SGE bug that causes qacct to occasionally return corrupt results.
        """
        d = _qacct_raw(task)

        job_failed = d['failed'][0] != '0'
        data_are_corrupt = _is_corrupt(d)

        if job_failed or data_are_corrupt:
            task.workflow.log.warn('%s SGE (qacct -j %s) reports %s:\n%s' %
                                   (task, task.drm_jobID,
                                    'corrupt data' if data_are_corrupt else 'job failure',
                                    json.dumps(d, indent=4, sort_keys=True)))

        processed_data = dict(
            exit_status=int(d['exit_status']) if not job_failed else int(re.search(r'^(\d+)', d['failed']).group(1)),

            percent_cpu=div(float(d['cpu']), float(d['ru_wallclock'])),
            wall_time=float(d['ru_wallclock']),

            cpu_time=float(d['cpu']),
            user_time=float(d['ru_utime']),
            system_time=float(d['ru_stime']),

            avg_rss_mem=d['ru_ixrss'],
            max_rss_mem_kb=convert_size_to_kb(d['ru_maxrss']),
            avg_vms_mem_kb=None,
            max_vms_mem_kb=convert_size_to_kb(d['maxvmem']),

            io_read_count=int(d['ru_inblock']),
            io_write_count=int(d['ru_oublock']),
            io_wait=float(d['iow']),
            io_read_kb=float(d['io']),
            io_write_kb=float(d['io']),

            ctx_switch_voluntary=int(d['ru_nvcsw']),
            ctx_switch_involuntary=int(d['ru_nivcsw']),

            avg_num_threads=None,
            max_num_threads=None,

            avg_num_fds=None,
            max_num_fds=None,

            memory=float(d['mem']),
        )

        return processed_data, data_are_corrupt

    def kill(self, task):
        """Terminate a task."""
        raise NotImplementedError

    def kill_tasks(self, tasks):
        for group in grouper(50, tasks):
            group = filter(lambda x: x is not None, group)
            pids = ','.join(map(lambda t: unicode(t.drm_jobID), group))
            subprocess.call(['qdel', pids], preexec_fn=exit_process_group)


def _is_corrupt(qacct_dict):
    """
    qacct may return multiple records for a job. They may all be corrupt. Yuk.

    This was allegedly fixed in 6.0u10 but we've seen it in UGE 8.3.1.

    http://osdir.com/ml/clustering.gridengine.users/2007-11/msg00397.html

    When multiple records are returned, the first one(s) may have corrupt data.
    UPDATE: this can happen even when only one block is returned, and we've also
    seen cases where multiple blocks are returned and not one is reliable. This
    function checks for values whose presence means an entire block is corrupt.

    Note that qacct may return a date that precedes the Epoch (!), depending on
    the $TZ env. variable. To be safe we check for dates within 24 hours of it.
    """
    return (qacct_dict.get('qsub_time', '').startswith('12/31/1969') or
            qacct_dict.get('qsub_time', '').startswith('01/01/1970') or
            qacct_dict.get('start_time', None) == '-/-' or
            qacct_dict.get('end_time', None) == '-/-') and \
           ("before writing exit_status" not in qacct_dict.get('failed', ''))


def _qacct_raw(task, timeout=600, quantum=15):
    """
    Parse qacct output into key/value pairs.

    If qacct reports results in multiple blocks (separated by a row of ===='s),
    the most recently-generated block with valid data is returned. If no such
    block exists, then return the most recently-generated block of corrupt data.
    """
    start = time.time()
    curr_qacct_dict = None
    good_qacct_dict = None
    num_retries = int(timeout / quantum)

    for i in xrange(num_retries):
        qacct_returncode = 0
        try:
            qacct_stdout_str, qacct_stderr_str = check_output_and_stderr(
                ['qacct', '-j', unicode(task.drm_jobID)],
                preexec_fn=exit_process_group)
            if qacct_stdout_str.strip():
                break
        except DetailedCalledProcessError as err:
            qacct_stdout_str = err.output.strip()
            qacct_stderr_str = err.stderr.strip()
            qacct_returncode = err.returncode

        if qacct_stderr_str and re.match(r'error: job id \d+ not found', qacct_stderr_str):
            if i > 0:
                task.workflow.log.info('%s SGE (qacct -j %s) reports "not found"; this may mean '
                                       'qacct is merely slow, or %s died in the \'qw\' state',
                                       task, task.drm_jobID, task.drm_jobID)
        else:
            task.workflow.log.error('%s SGE (qacct -j %s) returned error code %d',
                                    task, task.drm_jobID, qacct_returncode)
            if qacct_stdout_str or qacct_stderr_str:
                task.workflow.log.error('%s SGE (qacct -j %s) printed the following', task, task.drm_jobID)
                if qacct_stdout_str:
                    task.workflow.log.error('stdout: "%s"', qacct_stdout_str)
                if qacct_stderr_str:
                    task.workflow.log.error('stderr: "%s"', qacct_stderr_str)

        if i > 0:
            task.workflow.log.info(
                '%s SGE (qacct -j %s) attempt %d failed %d sec after first attempt%s',
                task, task.drm_jobID, i + 1, time.time() - start,
                '. Will recheck job status after %d sec' % quantum if i + 1 < num_retries else '')
        if i + 1 < num_retries:
            sleep_through_signals(timeout=quantum)
    else:
        # fallthrough: all retries failed
        raise ValueError('No valid `qacct -j %s` output after %d tries and %d sec' %
                         (task.drm_jobID, i, time.time() - start))

    for line in qacct_stdout_str.strip().split('\n'):
        if line.startswith('='):
            if curr_qacct_dict and not _is_corrupt(curr_qacct_dict):
                #
                # Cache this non-corrupt block of qacct data just
                # in case all the more recent blocks are corrupt.
                #
                good_qacct_dict = curr_qacct_dict

            curr_qacct_dict = OrderedDict()
            continue

        try:
            k, v = re.split(r'\s+', line, maxsplit=1)
        except ValueError:
            raise EnvironmentError('%s with drm_jobID=%s has unparseable qacct output:\n%s' %
                                   (task, task.drm_jobID, qacct_stdout_str))

        curr_qacct_dict[k] = v.strip()

    # if the last block of qacct data looks good, promote it
    if curr_qacct_dict and not _is_corrupt(curr_qacct_dict):
        good_qacct_dict = curr_qacct_dict

    return good_qacct_dict if good_qacct_dict else curr_qacct_dict


def _qstat_all():
    """
    returns a dict keyed by lsf job ids, who's values are a dict of bjob
    information about the job
    """
    try:
        lines = subprocess.check_output(['qstat'], preexec_fn=exit_process_group).decode().strip().split('\n')
    except (subprocess.CalledProcessError, OSError):
        return {}
    keys = re.split(r"\s+", lines[0])
    bjobs = {}
    for l in lines[2:]:
        items = re.split(r"\s+", l.strip())
        bjobs[items[0]] = dict(zip(keys, items))
    return bjobs
