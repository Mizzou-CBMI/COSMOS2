import json
import logging
import os
import re
import sys
import time
from collections import OrderedDict

from cosmos import TaskStatus
from cosmos.job.drm.DRM_Base import DRM
from cosmos.job.drm.util import convert_size_to_kb, div, exit_process_group, run_cli_cmd
from cosmos.util.signal_handlers import sleep_through_signals
from more_itertools import grouper


if os.name == "posix" and sys.version_info[0] < 3:
    import subprocess32 as subprocess
else:
    import subprocess


class QacctJobNotFoundError(Exception):
    pass


class DRM_GE(DRM):
    name = "ge"
    poll_interval = 5

    def submit_job(self, task):
        task.drm_jobID, task.status = qsub(
            cmd_fn=task.output_command_script_path,
            stdout_fn=task.output_stdout_path,
            stderr_fn=task.output_stderr_path,
            addl_args=task.drm_native_specification,
            drm_name=task.drm,
            logger=task.log,
            log_prefix=str(task),
        )

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
            qjobs = qstat()
            corrupt_data = {}

        for task in tasks:
            jid = str(task.drm_jobID)
            if jid not in qjobs or any(
                finished_state in qjobs[jid]["state"] for finished_state in ["e", "E"]
            ):
                #
                # If the job doesn't appear in qstat (or is tagged with 'e' or 'E'),
                # it *probably* has completed. However, SGE's qmaster may have
                # simply lost track of it for a little while, in which case qacct
                # will output corrupt data when it is interrogated.
                #
                try:
                    data, data_are_corrupt = self._get_task_return_data(task)
                    if data_are_corrupt:
                        task.workflow.log.warn(
                            "%s Corrupt SGE qstat/qacct output means it may still be running",
                            task,
                        )
                        corrupt_data[task] = data
                    else:
                        yield task, data
                except QacctJobNotFoundError:
                    # the job id didn't appear in qstat, but now it does; probably a qstat blip: let it run
                    if jid not in qjobs and jid in qstat():
                        task.workflow.log.warn(
                            "%s Went missing from qstat, but now appears to still be running",
                            task,
                        )
                    else:
                        raise

        num_cleanly_running_jobs = len(tasks) - len(corrupt_data)

        if num_cleanly_running_jobs > 0:
            for task in list(corrupt_data.keys()):
                task.workflow.log.info(
                    "%s Temporarily masking corrupt SGE output since %d other jobs are running cleanly"
                    % (task, num_cleanly_running_jobs)
                )
        else:
            for task, data in list(corrupt_data.items()):
                task.workflow.log.error(
                    "%s All outstanding drm_ge tasks have corrupt SGE output: giving up on this one"
                    % task
                )
                yield task, data

    def drm_statuses(self, tasks):
        """
        :param tasks: tasks that have been submitted to the job manager
        :returns: (dict) task.drm_jobID -> drm_status
        """
        if tasks:
            qjobs = qstat(logger=tasks[0].workflow.log)

            def f(task):
                return qjobs.get(str(task.drm_jobID), dict()).get(
                    "state", "UNK_JOB_STATE"
                )

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
        d = self.task_qacct(task)

        job_failed = d["failed"][0] != "0"
        data_are_corrupt = is_corrupt(d)

        if job_failed or data_are_corrupt:
            task.workflow.log.warn(
                "%s SGE (qacct -j %s) reports %s:\n%s"
                % (
                    task,
                    task.drm_jobID,
                    "corrupt data" if data_are_corrupt else "job failure",
                    json.dumps(d, indent=4, sort_keys=True),
                )
            )

        processed_data = dict(
            exit_status=int(d["exit_status"])
            if not job_failed
            else int(re.search(r"^(\d+)", d["failed"]).group(1)),
            percent_cpu=div(float(d["cpu"]), float(d["ru_wallclock"])),
            wall_time=float(d["ru_wallclock"]),
            cpu_time=float(d["cpu"]),
            user_time=float(d["ru_utime"]),
            system_time=float(d["ru_stime"]),
            avg_rss_mem=d["ru_ixrss"],
            max_rss_mem_kb=convert_size_to_kb(d["maxrss"]),
            avg_vms_mem_kb=None,
            max_vms_mem_kb=convert_size_to_kb(d["maxvmem"]),
            io_read_count=int(d["ru_inblock"]),
            io_write_count=int(d["ru_oublock"]),
            io_wait=float(d["iow"]),
            io_read_kb=convert_size_to_kb("%fG" % float(d["io"])),
            io_write_kb=convert_size_to_kb("%fG" % float(d["io"])),
            ctx_switch_voluntary=int(d["ru_nvcsw"]),
            ctx_switch_involuntary=int(d["ru_nivcsw"]),
            avg_num_threads=None,
            max_num_threads=None,
            avg_num_fds=None,
            max_num_fds=None,
            memory=float(d["mem"]),
        )

        return processed_data, data_are_corrupt

    @staticmethod
    def task_qacct(task, num_retries=10, quantum=30):
        """
        Return qacct data for the specified task.
        """
        return qacct(task.drm_jobID, num_retries, quantum, task.workflow.log, str(task))

    def kill(self, task):
        """Terminate a task."""
        self.kill_tasks([task])

    def kill_tasks(self, tasks):
        logger = tasks[0].workflow.log if tasks else _get_null_logger()

        for group in grouper(50, tasks):
            group = [x for x in group if x is not None]
            job_ids = [str(t.drm_jobID) for t in group]
            qdel(job_ids, logger=logger)


def _get_null_logger():
    """
    Return a logger that drops all messages passed to it.
    """
    logger = logging.getLogger(
        ".".join([sys.modules[__name__].__name__, "null_logger"])
    )
    # only initialize the null logger the first time we load it
    if not logger.handlers:
        logger.addHandler(logging.NullHandler())


def is_corrupt(qacct_dict):
    """
    Return true if qacct returns bogus job data for a job id.

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
    return (
        qacct_dict.get("qsub_time", "").startswith("12/31/1969")
        or qacct_dict.get("qsub_time", "").startswith("01/01/1970")
        or qacct_dict.get("start_time", None) == "-/-"
        or qacct_dict.get("end_time", None) == "-/-"
    ) and ("before writing exit_status" not in qacct_dict.get("failed", ""))


def qacct(job_id, num_retries=10, quantum=30, logger=None, log_prefix=""):
    """
    Parse qacct output into key/value pairs.

    If qacct reports results in multiple blocks (separated by a row of ===='s),
    the most recently-generated block with valid data is returned. If no block
    with valid data exists, then return the most recently-generated block of
    corrupt data. Call ``is_corrupt()`` on the output of this method to see if
    the data are suitable for use.
    """
    if not logger:
        logger = _get_null_logger()

    start = time.time()
    curr_qacct_dict = None
    good_qacct_dict = None

    for i in range(num_retries):

        qacct_stdout_str, qacct_stderr_str, qacct_returncode = run_cli_cmd(
            ["qacct", "-j", str(job_id)], logger=logger
        )
        if qacct_returncode == 0 and qacct_stdout_str.strip():
            # qacct returned actual output w/no error code. we're good
            break

        if qacct_stderr_str and re.match(
            r"error: job id \d+ not found", qacct_stderr_str
        ):
            if i > 0:
                logger.info(
                    '%s SGE (qacct -j %s) reports "not found"; this may mean '
                    "qacct is merely slow, or %s died in the 'qw' state",
                    log_prefix,
                    job_id,
                    job_id,
                )
        else:
            logger.error(
                "%s SGE (qacct -j %s) returned error code %d",
                log_prefix,
                job_id,
                qacct_returncode,
            )
            if qacct_stdout_str or qacct_stderr_str:
                logger.error(
                    "%s SGE (qacct -j %s) printed the following", log_prefix, job_id
                )
                if qacct_stdout_str:
                    logger.error('stdout: "%s"', qacct_stdout_str)
                if qacct_stderr_str:
                    logger.error('stderr: "%s"', qacct_stderr_str)

        if i > 0:
            logger.info(
                "%s SGE (qacct -j %s) attempt %d failed %d sec after first attempt%s",
                log_prefix,
                job_id,
                i + 1,
                time.time() - start,
                ". Will recheck job status after %d sec" % quantum
                if i + 1 < num_retries
                else "",
            )
        if i + 1 < num_retries:
            logger.info(
                "%s Will wait %d sec before calling qacct on %s again",
                log_prefix,
                quantum,
                job_id,
            )
            sleep_through_signals(timeout=quantum)
    else:
        # fallthrough: all retries failed
        raise QacctJobNotFoundError(
            "%s No valid SGE (qacct -j %s) output after %d tries over %d sec"
            % (log_prefix, job_id, i, time.time() - start)
        )

    for line in qacct_stdout_str.strip().split("\n"):
        if line.startswith("="):
            if curr_qacct_dict and not is_corrupt(curr_qacct_dict):
                #
                # Cache this non-corrupt block of qacct data just
                # in case all the more recent blocks are corrupt.
                #
                good_qacct_dict = curr_qacct_dict

            curr_qacct_dict = OrderedDict()
            continue

        try:
            k, v = re.split(r"\s+", line, maxsplit=1)
        except ValueError:
            raise EnvironmentError(
                "%s SGE (qacct -j %s) output is unparseable:\n%s"
                % (log_prefix, job_id, qacct_stdout_str)
            )

        curr_qacct_dict[k] = v.strip()

    # if the last block of qacct data looks good, promote it
    if curr_qacct_dict and not is_corrupt(curr_qacct_dict):
        good_qacct_dict = curr_qacct_dict

    return good_qacct_dict if good_qacct_dict else curr_qacct_dict


def qdel(job_ids, logger):
    """
    Call qdel on all the supplied job_ids: if that fails, qdel each job_id individually.

    Unlike other SGE cli commands, each qdel call is attempted only once, with a
    fairly harsh 20-second timeout, because this function is often called in an
    exit handler that does not have arbitrary amounts of time in which to run.
    """
    stdout, stderr, returncode = run_cli_cmd(
        ["qdel", "-f", ",".join(job_ids)], logger=logger, attempts=1, timeout=20,
    )
    if returncode == 0:
        logger.info("qdel reported success against %d job_ids", len(job_ids))
        return len(job_ids)

    successful_qdels = 0
    undead_job_ids = []

    for job_id in job_ids:
        if "has deleted job %s" % job_id in stdout:
            successful_qdels += 1
        elif "has registered the job %s for deletion" % job_id in stdout:
            successful_qdels += 1
        else:
            undead_job_ids.append(job_id)

    if undead_job_ids:
        #
        # If the original qdel didn't catch everything, kick off a qdel for each
        # remaining job id. Don't set a timeout and don't check the return code.
        #
        logger.warning(
            "qdel returned exit code %s, calling on one job_id at a time", returncode
        )

        for i, job_id in enumerate(undead_job_ids):
            logger.warning("will qdel %s in %d sec and ignore exit code", job_id, i)
            subprocess.Popen("sleep %d; qdel -f %s" % (i, job_id), shell=True)

    logger.info(
        "qdel reported success against %d of %d job_ids, see above for details",
        successful_qdels,
        len(job_ids),
    )
    return successful_qdels


def qstat(logger=None):
    """
    Return a mapping of job ids to a dict of GE information about each job.

    If qstat hangs or returns an error, wait 30 sec and call it again. Do this
    three times. If the final attempt returns an error, log it, and return an
    empty dictionary, which is the same behavior you'd get if all known jobs
    had exited. (If qstat is down for 90+ sec, any running job is likely to be
    functionally dead.)

    The exact contents of the sub-dictionaries in the returned dictionary's
    values() depend on the installed GE version.
    """
    if logger is None:
        logger = _get_null_logger()

    stdout, _, returncode = run_cli_cmd(
        ["qstat"], attempts=3, interval=30, logger=logger, timeout=30
    )
    if returncode != 0:
        logger.warning("qstat returned %s: If GE is offline, all jobs are dead or done")
        return {}
    lines = stdout.strip().split("\n")
    if not lines:
        logger.info(
            "qstat returned 0 and no output: all jobs are probably done, "
            "but in rare cases this may be a sign that GE is not working properly"
        )
        return {}

    keys = re.split(r"\s+", lines[0])
    bjobs = {}
    for l in lines[2:]:
        items = re.split(r"\s+", l.strip())
        bjobs[items[0]] = dict(list(zip(keys, items)))
    return bjobs


def qsub(
    cmd_fn,
    stdout_fn,
    stderr_fn,
    addl_args=None,
    drm_name="GE",
    logger=None,
    log_prefix="",
):
    """
    Submit the requested (bash-parseable) script stored in cmd_fn to GE.

    The command is submitted relatove to the current CWD. Callers should change
    this before calling if they need to run in a particular directory.

    Output will be written to two filenames, specified in stdout_fn and stderr_fn.
    Additional arguments to SGE may be specified as a single string in addl_args.
    Callers can optionally supply a logger object and a prefix to prepend to log messages.
    """
    for p in [stdout_fn, stderr_fn]:
        if os.path.exists(p):
            os.unlink(p)

    qsub_cli = "qsub -terse -o {stdout_fn} -e {stderr_fn} -b y -w e -cwd -S /bin/bash -V".format(
        stdout_fn=stdout_fn, stderr_fn=stderr_fn
    )

    if addl_args:
        qsub_cli += " %s" % addl_args

    job_id = None

    stdout, stderr, returncode = run_cli_cmd(
        '{qsub_cli} "{cmd_fn}"'.format(cmd_fn=cmd_fn, qsub_cli=qsub_cli),
        attempts=1,  # make just one attempt: running a task 2x could be disastrous
        env=os.environ,
        logger=logger,
        shell=True,
    )

    if returncode != 0:
        logger.error(
            "%s submission to %s (%s) failed with error %s",
            log_prefix,
            drm_name,
            qsub,
            returncode,
        )
        status = TaskStatus.failed
    else:
        try:
            job_id = str(int(stdout))
        except ValueError:
            logger.error(
                "%s submission to %s returned unexpected text: %s",
                log_prefix,
                drm_name,
                stdout,
            )
            status = TaskStatus.failed
        else:
            status = TaskStatus.submitted

    return (job_id, status)
