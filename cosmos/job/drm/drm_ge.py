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

        drm_jobID = int(re.search('job (\d+) ', out).group(1))
        return drm_jobID

    def filter_is_done(self, tasks):
        if len(tasks):
            qjobs = qstat_all()
        for task in tasks:
            jid = str(task.drm_jobID)
            if jid not in qjobs:
                # print 'missing %s %s' % (task, task.drm_jobID)
                yield task, self._get_task_return_data(task)
            else:
                if any(finished_state in qjobs[jid]['state'] for finished_state in ['e', 'E']):
                    yield task, self._get_task_return_data(task)

    def drm_statuses(self, tasks):
        """
        :param tasks: tasks that have been submitted to the job manager
        :returns: (dict) task.drm_jobID -> drm_status
        """
        if len(tasks):
            qjobs = qstat_all()

            def f(task):
                return qjobs.get(str(task.drm_jobID), dict()).get('state', '???')

            return {task.drm_jobID: f(task) for task in tasks}
        else:
            return {}

    def _get_task_return_data(self, task):
        d = qacct(task)
        failed = d['failed'][0] != '0'
        return dict(
            exit_status=int(d['exit_status']) if not failed else int(re.search('^(\d+)', d['failed']).group(1)),

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

    def kill(self, task):
        "Terminates a task"
        raise NotImplementedError

    def kill_tasks(self, tasks):
        for group in grouper(50, tasks):
            group = filter(lambda x: x is not None, group)
            pids = ','.join(map(lambda t: str(t.drm_jobID), group))
            sp.Popen(['qdel', pids], preexec_fn=preexec_function)


def is_garbage(qacct_dict):
    """
    qacct may return multiple records for a job. Yuk.

    This was allegedly fixed in 6.0u10 but we've seen it in UGE 8.3.1.

    http://osdir.com/ml/clustering.gridengine.users/2007-11/msg00397.html

    When multiple records are returned, the first one(s) may have garbage data.
    This function checks for three values whose presence means the entire block
    is wrong.
    """
    return qacct_dict.get('qsub_time', '').startswith('12/31/1969') or \
        qacct_dict.get('start_time', None) == '-/-' or \
        qacct_dict.get('end_time', None) == '-/-'


def qacct(task, timeout=600):
    start = time.time()
    qacct_dict = None

    with open(os.devnull, 'w') as DEVNULL:
        while True:
            if time.time() - start > timeout:
                raise ValueError('Could not qacct -j %s' % task.drm_jobID)
            try:
                qacct_out = sp.check_output(['qacct', '-j', str(task.drm_jobID)], stderr=DEVNULL)
                if len(qacct_out.strip()):
                    break
                else:
                    task.workflow.log.warn('`qacct -j %s` returned an empty string for %s' % (task.drm_jobID, task))
            except sp.CalledProcessError:
                pass
            time.sleep(5)

    for line in qacct_out.strip().split('\n'):
        if line.startswith('='):
            if not qacct_dict or is_garbage(qacct_dict):
                # Whether we haven't parsed any qacct data yet, or everything
                # we've seen up to this point is unreliable garbage, when we see
                # a stretch of ==='s, a new block of qacct data is beginning.
                qacct_dict = OrderedDict()
                continue
            else:
                break
        try:
            k, v = re.split(r'\s+', line, maxsplit=1)
        except ValueError:
            raise EnvironmentError('%s with drm_jobID=%s has invalid qacct output: %s' %
                                   (task, task.drm_jobID, qacct_out))

        qacct_dict[k] = v.strip()

    return qacct_dict


def qstat_all():
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
