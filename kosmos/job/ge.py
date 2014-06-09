import subprocess as sp
import re
import os

class DRM_GE():
    """
    Stateless
    """
    name = 'lsf'

    def __init__(self, jobmanager):
        self.jobmanager = jobmanager

    def submit_job(self, task):
        ns = ' ' + task.drmaa_native_specification if task.drmaa_native_specification else ''
        qsub = 'qsub -o {stdout} -e {stderr} -b y{ns} '.format(stdout=task.output_stdout_path,
                                                          stderr=task.output_stderr_path,
                                                          ns=ns)

        out = sp.check_output('{qsub} "{cmd_str}"'.format(cmd_str=self.jobmanager.get_command_str(task), bsub=qsub),
                              env=os.environ,
                              preexec_fn=preexec_function(),
                              shell=True)

        task.drmaa_jobID = int(re.search('job (\d+) ', out).group(1))

    def filter_is_done(self, tasks):
        if len(tasks):
            bjobs = qstat_all()

            def f(task):
                jid = str(task.drmaa_jobID)
                if jid not in bjobs:
                    print 'missing %s %s' % (task, task.drmaa_jobID)
                    return False
                else:
                    return bjobs[jid]['state'] in ['DONE', 'EXIT', 'UNKWN', 'ZOMBI']

            return filter(f, tasks)
        else:
            return []

    def drm_statuses(self, tasks):
        """
        :param tasks: tasks that have been submitted to the job manager
        :returns: (dict) task.drmaa_jobID -> drm_status
        """
        if len(tasks):
            bjobs = qstat_all()

            def f(task):
                return bjobs.get(str(task.drmaa_jobID), dict()).get('state', '???')

            return {task.drmaa_jobID: f(task) for task in tasks}
        else:
            return {}


    def kill(self, task):
        "Terminates a task"
        os.system('qdel %s' % task.drmaa_jobID)


# def _bjobs(task):
    # lines = sp.check_output(['bjobs', str(task.drmaa_jobID)]).split("\n")
    # header = re.split("\s\s+", lines[0])
    # items = re.split("\s\s+", lines[1])
    # return dict(zip(header, items))


def qstat_all():
    """
    returns a dict keyed by lsf job ids, who's values are a dict of bjob
    information about the job
    """
    try:
        lines = sp.check_output(['qstat']).split('\n')
    except (sp.CalledProcessError, OSError):
        return {}
    bjobs = {}
    header = re.split("\s\s+", lines[0])
    for l in lines[1:]:
        items = re.split("\s\s+", l.strip())
        bjobs[items[0]] = dict(zip(header, items))
    return bjobs


def preexec_function():
    # Ignore the SIGINT signal by setting the handler to the standard
    # signal handler SIG_IGN.  This allows Kosmos to cleanly
    # terminate jobs when there is a ctrl+c event
    os.setpgrp()