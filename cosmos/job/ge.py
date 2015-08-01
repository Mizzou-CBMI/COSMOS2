import subprocess as sp
import re
import os
from collections import OrderedDict
import time

from ..util.iterstuff import grouper
from .drm import DRM


class DRM_GE(DRM):
    name = 'ge'

    def submit_job(self, task):
        ns = ' ' + task.drm_native_specification if task.drm_native_specification else ''
        qsub = 'qsub -o {stdout} -e {stderr} -b y -cwd -S /bin/bash -V{ns} '.format(stdout=task.output_stdout_path,
                                                                                    stderr=task.output_stderr_path,
                                                                                    ns=ns)

        out = sp.check_output('{qsub} "{cmd_str}"'.format(cmd_str=task.output_command_script_path, qsub=qsub),
                              env=os.environ,
                              preexec_fn=preexec_function,
                              shell=True)

        task.drm_jobID = int(re.search('job (\d+) ', out).group(1))

    def filter_is_done(self, tasks):
        if len(tasks):
            qjobs = qstat_all()

            def f(task):
                jid = str(task.drm_jobID)
                if jid not in qjobs:
                    # print 'missing %s %s' % (task, task.drm_jobID)
                    return True
                else:
                    if any(finished_state in qjobs[jid]['state'] for finished_state in ['e', 'E']):
                        return True

            return filter(f, tasks)
        else:
            return []

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


    def get_task_return_data(self, task):
        d = qacct(task)
        return dict(
            exit_status=int(d['exit_status']),
            percent_cpu=float(d['cpu']),
            user_time=d['ru_utime'],
            system_time=d['ru_stime'],
            wall_time=d['ru_wallclock']
        )
        # exit_status = Column(Integer)
        #
        # percent_cpu = Column(Integer)
        # wall_time = Column(BigInteger)
        #
        # cpu_time = Column(BigInteger)
        # user_time = Column(BigInteger)
        # system_time = Column(BigInteger)

    def kill(self, task):
        "Terminates a task"
        raise NotImplementedError

    def kill_tasks(self, tasks):
        for group in grouper(tasks, 50):
            group = filter(lambda x: x is not None, group)
            pids = ','.join(map(lambda t: str(t.drm_jobID), group))
            sp.Popen(['qdel', pids], preexec_fn=preexec_function)


def qacct(task):
    start = time.time()
    out = None
    with open(os.devnull, 'w') as DEVNULL:
        while time.time() - start < 100:
            try:
                out = sp.check_output(['qacct', '-j', str(task.drm_jobID)], stderr=DEVNULL)
                break
            except sp.CalledProcessError:
                pass
            time.sleep(1)

    if out is None:
        raise ValueError('Could not qacct -j %s' % task.drm_jobID)

    def g():
        for line in out.strip().split('\n')[1:]: # first line is a header
            yield line.split()

    return OrderedDict(g())


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