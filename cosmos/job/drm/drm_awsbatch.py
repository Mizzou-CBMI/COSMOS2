import logging
import os
import pprint
import random
import re
import string
import time
from concurrent.futures.thread import ThreadPoolExecutor

import boto3
import more_itertools
from botocore.config import Config

from cosmos.api import TaskStatus
from cosmos.job.drm.DRM_Base import DRM
from cosmos.util.helpers import progress_bar

MAX_THREADS = 25
BOTO_CONFIG = Config(retries=dict(max_attempts=10, mode="adaptive"), max_pool_connections=25)


def random_string(length):
    return "".join([random.choice(string.ascii_letters + string.digits) for _ in range(length)])


def split_bucket_key(s3_uri):
    """
    >>> split_bucket_key('s3://bucket/path/to/fname')
    ('bucket', 'path/to/fname')
    """
    bucket, key = re.search("s3://(.+?)/(.+)", s3_uri).groups()
    if key == "":
        raise ValueError("no prefix in %s" % s3_uri)
    return bucket, key


def submit_script_as_aws_batch_job(
    local_script_path,
    s3_prefix_for_command_script_temp_files,
    job_name,
    job_def_arn,
    job_queue,
    instance_type=None,
    memory=1024,
    vpu_req=1,
    gpu_req=None,
    environment=None,
):
    """
    :param local_script_path: the local path to a script to run in awsbatch.
    :param s3_prefix_for_command_script_temp_files: the s3 bucket to use for storing the local script to to run.  Caller
      is responsible for cleaning it up.
    :param job_name: name of the job_dict.
    :param container_image: docker image.
    :param memory: amount of memory to reserve.
    :param vpu_req: amount of vcpus to reserve.
    :param environment: {env_name -> env_val} environment variables to set
    :return: obId, job_definition_arn, s3_command_script_uri.
    """
    if environment is None:
        environment = dict()

    if " " in job_name or ":" in job_name:
        raise ValueError("job_name `%s` is invalid" % job_name)
    if s3_prefix_for_command_script_temp_files.endswith("/"):
        raise ValueError(
            "s3_prefix_for_command_script_temp_files should not have a "
            "trailing slash.  It is set to %s" % s3_prefix_for_command_script_temp_files
        )
    if not s3_prefix_for_command_script_temp_files.startswith("s3://"):
        raise ValueError("invalid s3_prefix_for_command_script_temp_files: %s" % s3_prefix_for_command_script_temp_files)

    batch = boto3.client(service_name="batch", config=BOTO_CONFIG)
    s3 = boto3.client(service_name="s3", config=BOTO_CONFIG)

    bucket, key = split_bucket_key(s3_prefix_for_command_script_temp_files)
    key = os.path.join(key, random_string(32) + "." + job_name + ".script")
    s3.upload_file(local_script_path, bucket, key)
    s3_command_script_uri = "s3://" + os.path.join(bucket, key)

    # I could optionally add glances --stdout-csv here
    # which would save the resource data to a s3_command_script_uri.resources.csv
    # which i could parse on cleanup.
    # This would require that the image have glances installed though, obviously.
    command = "aws s3 cp --quiet {s3_command_script_uri} command_script && " "chmod +x command_script && " "./command_script"
    command = command.format(**locals())

    container_overrides = {
        "resourceRequirements": [],
        "environment": [{"name": key, "value": val} for key, val in list(environment.items())],
        # run_s3_script
        "command": ["bash", "-c", command],
    }
    if memory is not None:
        container_overrides["memory"] = memory
    if vpu_req is not None:
        container_overrides["vcpus"] = vpu_req
    if instance_type is not None:
        container_overrides["instanceType"] = instance_type
    if gpu_req is not None and gpu_req != 0:
        container_overrides["resourceRequirements"].append({"value": str(gpu_req), "type": "GPU"})
        visible_devices = ",".join(map(str, list(range(gpu_req))))
        container_overrides["environment"].append({"name": "CUDA_VISIBLE_DEVICES", "value": visible_devices})

    submit_jobs_response = batch.submit_job(
        jobName=job_name, jobQueue=job_queue, jobDefinition=job_def_arn, containerOverrides=container_overrides,
    )
    jobId = submit_jobs_response["jobId"]

    return jobId, s3_command_script_uri


def get_logs(log_stream_name, attempts=9, sleep_between_attempts=10):
    logs_client = boto3.client(service_name="logs", config=BOTO_CONFIG)
    try:
        response = logs_client.get_log_events(logGroupName="/aws/batch/job", logStreamName=log_stream_name, startFromHead=True)
        _check_aws_response_for_error(response)
        return "\n".join(d["message"] for d in response["events"])
    except logs_client.exceptions.ResourceNotFoundException:
        if attempts == 1:
            return "log stream not found for log_stream_name: %s\n" % log_stream_name
        else:
            time.sleep(sleep_between_attempts)
            return get_logs(log_stream_name, attempts=attempts - 1, sleep_between_attempts=sleep_between_attempts,)


class JobStatusMismatchError(Exception):
    pass


def _get_aws_batch_job_infos_for_batch(job_ids, batch_client):
    # ensure that the list of job ids is unique
    assert len(job_ids) == len(set(job_ids))
    describe_jobs_response = batch_client.describe_jobs(jobs=job_ids)
    _check_aws_response_for_error(describe_jobs_response)
    returned_jobs = sorted(describe_jobs_response["jobs"], key=lambda job: job_ids.index(job["jobId"]))
    if sorted([job["jobId"] for job in returned_jobs]) != sorted(job_ids):
        raise JobStatusMismatchError()
    return returned_jobs


def get_aws_batch_job_infos(all_job_ids, logger):
    # ensure that the list of job ids is unique
    assert len(all_job_ids) == len(set(all_job_ids))
    batch_client = boto3.client(service_name="batch", config=BOTO_CONFIG)
    returned_jobs = []
    for batch_job_ids in more_itertools.chunked(all_job_ids, 50):
        while True:
            try:
                batch_returned_jobs = _get_aws_batch_job_infos_for_batch(batch_job_ids, batch_client)
            except JobStatusMismatchError:
                logger.warning("aws batch describe-jobs returned different jobs than were passed. Re-trying.")
                continue
            else:
                returned_jobs.extend(batch_returned_jobs)
                break
    returned_ids = [job["jobId"] for job in returned_jobs]
    assert sorted(returned_ids) == sorted(all_job_ids), str(set(returned_ids) - set(all_job_ids)) + str(
        set(all_job_ids) - set(returned_ids)
    )
    return returned_jobs


def register_base_job_definition(container_image, environment, command):
    # register base job definition
    container_properties = {
        "image": container_image,
        "jobRoleArn": "ecs_administrator",
        "mountPoints": [{"containerPath": "/scratch", "readOnly": False, "sourceVolume": "scratch"}],
        "volumes": [{"name": "scratch", "host": {"sourcePath": "/scratch"}}],
        "resourceRequirements": [],
        # run_s3_script
        "command": ["bash", "-c", command],
        "memory": 100,
        "vcpus": 1,
        "privileged": True,
    }

    if environment:
        container_properties["environment"]: [{"name": key, "value": val} for key, val in environment.items()]

    batch = boto3.client(service_name="batch", config=BOTO_CONFIG)
    resp = batch.register_job_definition(
        jobDefinitionName="cosmos_base_job_definition", type="container", containerProperties=container_properties,
    )
    _check_aws_response_for_error(resp)
    job_definition_arn = resp["jobDefinitionArn"]

    return job_definition_arn


class DRM_AWSBatch(DRM):
    name = "awsbatch"
    required_drm_options = {
        "container_image",
        "s3_prefix_for_command_script_temp_files",
        "retry_only_if_status_reason_matches",  # ex: "host_terminated"
    }

    _batch_client = None
    _s3_client = None
    logger = None

    def __init__(self, log):
        self.job_id_to_s3_script_uri = dict()
        super(DRM_AWSBatch, self).__init__(log)

        self.image_to_job_definition = {}

    def __del__(self):
        for image, job_definition_arn in self.image_to_job_definition.items():
            # self.log.info(f"Deregistering job definition for image: {image}")
            self.batch_client.deregister_job_definition(jobDefinition=job_definition_arn)

    @property
    def batch_client(self):
        if self._batch_client is None:
            self._batch_client = boto3.client(service_name="batch", config=BOTO_CONFIG)
        return self._batch_client

    @property
    def s3_client(self):
        if self._s3_client is None:
            self._s3_client = boto3.client(service_name="s3", config=BOTO_CONFIG)
        return self._s3_client

    def submit_job(self, task):
        raise NotImplementedError("use .submit_jobs()")

    def _submit_job(self, task):
        # THIS FUNCTION MUST WORK INSIDE A SEPARATE THREAD

        if task.queue is None:
            raise ValueError("task.queue cannot be None for %s" % task)
        if task.core_req is None:
            raise ValueError("task.core_req cannot be None for task %s" % task)
        if task.mem_req is None:
            raise ValueError("task.mem_req cannot be None for task %s" % task)

        job_name = "".join(
            [
                "cosmos-",
                task.stage.name.replace("/", "__").replace(":", ""),
                "__",
                task.uid.replace("/", "__").replace(":", ""),
            ]
        )[
            :128
        ]  # job names can be a maximum of 128 characters
        # task.workflow.log.info("Setting job name to: {}".format(job_name))

        job_def_arn = self.image_to_job_definition[task.drm_options["container_image"]]
        (jobId, s3_command_script_uri,) = submit_script_as_aws_batch_job(
            local_script_path=task.output_command_script_path,
            s3_prefix_for_command_script_temp_files=task.drm_options["s3_prefix_for_command_script_temp_files"],
            # container_image=task.drm_options["container_image"],
            job_def_arn=job_def_arn,
            job_name=job_name,
            job_queue=task.queue,
            memory=task.mem_req,
            vpu_req=task.cpu_req,
            gpu_req=task.gpu_req,
            instance_type=task.drm_options.get("instance_type"),
        )

        # just save pointer to logstream.  We'll collect them when the job finishes.
        # job_dict = get_aws_batch_job_infos([jobId], self.log)[0]  # , task.workflow.log)[0]
        with open(task.output_stdout_path, "w"):
            pass
        with open(task.output_stderr_path, "w") as fp:
            fp.write(pprint.pformat(dict(job_id=jobId), indent=2))

        return jobId, s3_command_script_uri, job_def_arn

    def submit_jobs(self, tasks):
        # Register job definitions for each container_image
        for container_image in set(task.drm_options["container_image"] for task in tasks):
            if container_image not in self.image_to_job_definition:
                self.log.info(f"Registering base job definition for image: {container_image}")
                self.image_to_job_definition[container_image] = register_base_job_definition(
                    container_image=container_image, environment=None, command="user-should-override-this",
                )

        if len(tasks) > 1:
            with ThreadPoolExecutor(min(len(tasks), MAX_THREADS)) as pool:
                rv = list(progress_bar(pool.map(self._submit_job, tasks), len(tasks), "Submitting"))
        else:
            # submit in serial without a progress bar
            rv = list(map(self._submit_job, tasks))

        for task, rv in zip(tasks, rv):
            jobId, s3_command_script_uri, job_definition_arn = rv

            # set task attributes
            task.drm_jobID = jobId
            task.status = TaskStatus.submitted
            task.s3_command_script_uri = s3_command_script_uri
            task.job_definition_arn = job_definition_arn

    def filter_is_done(self, tasks):
        job_ids = [task.drm_jobID for task in tasks]
        assert len(set(job_ids)) == len(job_ids)
        if len(job_ids) == 0:
            job_id_to_job_dict = dict()
        else:
            jobs = get_aws_batch_job_infos(job_ids, self.log)
            job_id_to_job_dict = {job["jobId"]: job for job in jobs}
        for task in tasks:
            job_dict = job_id_to_job_dict[task.drm_jobID]
            if job_dict["status"] in ["SUCCEEDED", "FAILED"]:
                # get exit status
                if "attempts" in job_dict:
                    attempt = job_dict["attempts"][-1]
                    # if re.search("Host EC2 .+ terminated.", attempt["statusReason"]):
                    #     # this job failed because the instance was shut down (presumably because it was a
                    #     # spot instance)
                    #     status_reason = "host_terminated"
                    # else:
                    status_reason = attempt.get("statusReason", None)
                    # exit code might be missing if for example the instance was terminated because the compute
                    # environment was deleted.
                    exit_status = attempt["container"].get("exitCode", -2)
                else:
                    status_reason = "no_attempt"
                    exit_status = -1

                if job_dict["status"] == "FAILED":
                    assert exit_status != 0, "%s failed, but has an exit_status of 0" % task

                self._cleanup_task(task, job_dict["container"]["logStreamName"])
                try:
                    wall_time = int(round((job_dict["stoppedAt"] - job_dict["startedAt"]) / 1000))
                except KeyError:
                    self.log.warning(f"Could not find timing info for job:'\n{job_dict}\n'")
                    wall_time = 0
                yield task, dict(exit_status=exit_status, wall_time=wall_time, status_reason=status_reason)

    def _cleanup_task(
        self, task, log_stream_name=None, get_log_attempts=12, get_log_sleep_between_attempts=10,
    ):
        # NOTE this code must be thread safe (cannot use any sqlalchemy)

        if get_log_attempts > 0:
            # if log_stream_name wasn't passed in, query aws to get it
            if log_stream_name is None:
                job_dict = get_aws_batch_job_infos([task.drm_jobID], self.log)
                log_stream_name = job_dict[0]["container"].get("logStreamName")

            if log_stream_name is None:
                logs = "no log stream was available for job: %s\n" % task.drm_jobID
            else:
                # write logs to stdout
                logs = get_logs(
                    log_stream_name=log_stream_name,
                    attempts=get_log_attempts,
                    sleep_between_attempts=get_log_sleep_between_attempts,
                )

            with open(task.output_stdout_path, "w") as fp:
                fp.write(
                    logs
                    + "\n"
                    + "WARNING: this might be truncated.  "
                    + "check log stream on the aws console for job: %s" % task.drm_jobID
                )

        # delete temporary s3 script path
        bucket, key = split_bucket_key(task.s3_command_script_uri)
        self.s3_client.delete_object(Bucket=bucket, Key=key)

    def drm_statuses(self, tasks):
        """
        :returns: (dict) task.drm_jobID -> drm_status
        """
        job_ids = [task.drm_jobID for task in tasks]
        if len(job_ids) == 0:
            return {}
        return {d["jobId"]: d["status"] for d in get_aws_batch_job_infos(job_ids, self.log)}

    def _terminate_task(self, task):
        # NOTE this code must be thread safe (cannot use any sqlalchemy)
        batch_client = boto3.client(service_name="batch", config=BOTO_CONFIG)
        # cancel_job_response = batch_client.cancel_job(jobId=task.drm_jobID, reason="terminated by cosmos")
        # _check_aws_response_for_error(cancel_job_response)

        terminate_job_response = batch_client.terminate_job(jobId=task.drm_jobID, reason="terminated by cosmos")
        _check_aws_response_for_error(terminate_job_response)

    def kill(self, task):
        # NOTE this code must be thread safe (cannot use any sqlalchemy)
        self._terminate_task(task)
        self._cleanup_task(task, get_log_attempts=0)

    def kill_tasks(self, tasks):
        if len(tasks):
            with ThreadPoolExecutor(min(len(tasks), MAX_THREADS)) as pool:
                self.log.info("Killing Tasks...")
                list(progress_bar(pool.map(self.kill, tasks), count=len(tasks), prefix="Killing "))


class JobStatusError(Exception):
    pass


def _check_aws_response_for_error(r):
    if "failures" in r and len(r["failures"]):
        raise Exception("Failures:\n{0}".format(pprint.pformat(r, indent=2)))

    status_code = r["ResponseMetadata"]["HTTPStatusCode"]
    if status_code != 200:
        raise Exception("Task status request received status code {0}:\n{1}".format(status_code, pprint.pformat(r, indent=2)))
