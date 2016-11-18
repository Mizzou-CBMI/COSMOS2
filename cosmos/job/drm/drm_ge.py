import subprocess as sp
import re
import os
from collections import OrderedDict
import time
from .util import div, convert_size_to_kb

from more_itertools import grouper
from .DRM_Base import DRM


class DRM_GE(DRM):
    name = 'ge'
    poll_interval = 5

    def submit_job(self, task):
        for p in [task.output_stdout_path, task.output_stderr_path]:
            if os.path.exists(p):
                os.unlink(p)

        ns = ' ' + task.drm_native_specification if task.drm_native_specification else ''
        qsub = 'qsub -o {stdout} -e {stderr} -b y -cwd -S /bin/bash -V{ns} '.format(stdout=task.output_stdout_path,
                                                                                    stderr=task.output_stderr_path,
                                                                                    ns=ns)

        out = sp.check_output('{qsub} "{cmd_str}"'.format(cmd_str=task.output_command_script_path, qsub=qsub),
                              env=os.environ, preexec_fn=preexec_function, shell=True)

        drm_jobID = unicode(re.search('job (\d+) ', out).group(1))
        return drm_jobID

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
        if len(tasks):
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
                        'corrupt qstat/qacct output for %s means it may still be running' % task)
                    corrupt_data[task] = data
                else:
                    yield task, data

        num_cleanly_running_jobs = len(tasks) - len(corrupt_data)

        if num_cleanly_running_jobs > 0:
            for task in corrupt_data.keys():
                task.workflow.log.info(
                    'temporarily masking corrupt SGE data for %s since %d other jobs are running cleanly' %
                    (task, num_cleanly_running_jobs))
        else:
            for task, data in corrupt_data.items():
                task.workflow.log.error(
                    'all outstanding drm_ge tasks had corrupt SGE data: giving up on %s' % task)
                yield task, data

    def drm_statuses(self, tasks):
        """
        :param tasks: tasks that have been submitted to the job manager
        :returns: (dict) task.drm_jobID -> drm_status
        """
        if len(tasks):
            qjobs = _qstat_all()

            def f(task):
                return qjobs.get(unicode(task.drm_jobID), dict()).get('state', '???')

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
            task.workflow.log.warn('`qacct -j %s` (for task %s) shows %s:\n%s' %
                                   (task.drm_jobID, task,
                                    'corrupt data' if data_are_corrupt else 'job failure',
                                    d))

        processed_data = dict(
            exit_status=int(d['exit_status']) if not job_failed else int(re.search('^(\d+)', d['failed']).group(1)),

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
        "Terminates a task"
        raise NotImplementedError

    def kill_tasks(self, tasks):
        for group in grouper(50, tasks):
            group = filter(lambda x: x is not None, group)
            pids = ','.join(map(lambda t: unicode(t.drm_jobID), group))
            sp.Popen(['qdel', pids], preexec_fn=preexec_function)


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
    return \
        qacct_dict.get('qsub_time', '').startswith('12/31/1969') or \
        qacct_dict.get('qsub_time', '').startswith('01/01/1970') or \
        qacct_dict.get('start_time', None) == '-/-' or \
        qacct_dict.get('end_time', None) == '-/-'


def _qacct_raw(task, timeout=600):
    """
    Parse qacct output into key/value pairs.

    If qacct reports results in multiple blocks (separated by a row of ===='s),
    the most recently-generated block with valid data is returned. If no such
    block exists, then return the most recently-generated block of corrupt data.
    """
    start = time.time()
    curr_qacct_dict = None
    good_qacct_dict = None

    with open(os.devnull, 'w') as DEVNULL:
        while True:
            if time.time() - start > timeout:
                raise ValueError('Could not qacct -j %s' % task.drm_jobID)
            try:
                qacct_out = sp.check_output(['qacct', '-j', unicode(task.drm_jobID)], stderr=DEVNULL)
                if len(qacct_out.strip()):
                    break
                else:
                    task.workflow.log.warn('`qacct -j %s` returned an empty string for %s' % (task.drm_jobID, task))
            except sp.CalledProcessError:
                pass
            time.sleep(5)

    for line in qacct_out.strip().split('\n'):
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
            raise EnvironmentError('%s with drm_jobID=%s has corrupt qacct output:\n%s' %
                                   (task, task.drm_jobID, qacct_out))

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
        lines = sp.check_output(['qstat'], preexec_fn=preexec_function).strip().split('\n')
    except (sp.CalledProcessError, OSError):
        return {}
    keys = re.split("\s+", lines[0])
    bjobs = {}
    for l in lines[2:]:
        items = re.split("\s+", l.strip())
        bjobs[items[0]] = dict(zip(keys, items))
    return bjobs


def preexec_function():
    # Ignore the SIGINT signal by setting the handler to the standard
    # signal handler SIG_IGN.  This allows Cosmos to cleanly
    # terminate jobs when there is a ctrl+c event
    os.setpgrp()
    return os.setsid
