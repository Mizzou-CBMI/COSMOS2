import random
import string
import sys

import boto3

if sys.version_info < (3,):
    pass
else:
    pass
import time

from cosmos.job.drm.DRM_Base import DRM
from cosmos.api import TaskStatus
import pprint

image = '638253504273.dkr.ecr.us-west-1.amazonaws.com/ravel:132223b90281fa81431053e96214bb21fabc90d9'
job_queue = 'pipe'
s3_bucket_for_command_scripts = 'ravel-cosmos'


def random_string(length):
    return ''.join([random.choice(string.ascii_letters + string.digits) for _ in range(length)])


def submit_aws_batch_job(local_script_path, s3_bucket_for_command_scripts, job_name, memory=1024, vcpus=1):
    batch = boto3.client(service_name="batch")
    s3 = boto3.client(service_name="s3")

    key = random_string(32) + '.txt'
    s3.upload_file(local_script_path, s3_bucket_for_command_scripts, key)
    s3_command_script_uri = 's3://{s3_bucket_for_command_scripts}/{key}'.format(
        s3_bucket_for_command_scripts=s3_bucket_for_command_scripts,
        key=key)

    container_properties = {
        "image": image,
        "jobRoleArn": "ecs_administrator",
        "mountPoints": [{"containerPath": "/scratch", "readOnly": False, "sourceVolume": "scratch"}],
        "volumes": [{"name": "scratch", "host": {"sourcePath": "/scratch"}}],
        "resourceRequirements": [],
        "command": ['run_s3_script', s3_command_script_uri]
    }
    if memory is not None:
        container_properties["memory"] = memory
        container_properties['vcpus'] = vcpus

    resp = batch.register_job_definition(
        jobDefinitionName=job_name,
        type='container',
        containerProperties=container_properties
    )
    _check_aws_response_for_error(resp)
    job_definition_arn = resp['jobDefinitionArn']

    submit_jobs_response = batch.submit_job(
        jobName='cosmos-job',  # add task.name
        jobQueue=job_queue,
        jobDefinition=job_definition_arn
    )
    jobId = submit_jobs_response['jobId']

    return jobId, job_definition_arn, s3_command_script_uri


def get_aws_batch_job_info(job_ids):
    batch_client = boto3.client(service_name="batch")
    describe_jobs_response = batch_client.describe_jobs(jobs=job_ids)
    _check_aws_response_for_error(describe_jobs_response)
    return describe_jobs_response['jobs']


class DRM_AWSBatch(DRM):
    name = 'awsbatch'
    poll_interval = 0.3

    def __init__(self):
        self.job_id_to_s3_script_uri = dict()
        self.batch_client = boto3.client(service_name="batch")
        self.s3_client = boto3.client(service_name="s3")
        super(DRM_AWSBatch, self).__init__()

    def submit_job(self, task):
        jobId, job_definition_arn, s3_command_script_uri = submit_aws_batch_job(task.output_command_script_path,
                                                                                s3_bucket_for_command_scripts,
                                                                                job_name='cosmos-{}-'.format(
                                                                                    task.stage.name),
                                                                                memory=task.mem_req,
                                                                                vcpus=task.cpu_req)
        task.drm_jobID = jobId
        task.status = TaskStatus.submitted

    def filter_is_done(self, tasks):
        job_ids = [task.drm_jobID for task in tasks]
        jobs = get_aws_batch_job_info(job_ids)
        for task, job in zip(tasks, jobs):
            if job['status'] in ['succeeded', 'failed']:
                if 'attempts' in job:
                    exit_status = job['attempts'][-1]['container']['exitCode']
                else:
                    exit_status = -1
                yield task, dict(exit_status=exit_status,
                                 wall_time=job['stoppedAt'] - job['startedAt'])

    def drm_statuses(self, tasks):
        """
        :returns: (dict) task.drm_jobID -> drm_status
        """
        job_ids = [task.drm_jobID for task in tasks]
        return dict(zip(job_ids, get_aws_batch_job_info(job_ids)))

    def _get_task_return_data(self, task):
        return dict(exit_status=self.procs[task.drm_jobID].wait(timeout=0),
                    wall_time=round(int(time.time() - self.procs[task.drm_jobID].start_time)))

    def kill(self, task):
        batch_client = boto3.client(service_name="batch")
        terminate_job_response = batch_client.terminate_job(jobId=task.drm_jobID,
                                                            reason='terminated by cosmos')
        _check_aws_response_for_error(terminate_job_response)


class JobStatusError(Exception):
    pass


def _check_aws_response_for_error(r):
    if 'failures' in r and len(r['failures']):
        raise Exception('Failures:\n{0}'.format(pprint.pformat(r, indent=2)))

    status_code = r['ResponseMetadata']['HTTPStatusCode']
    if status_code != 200:
        raise Exception(
            'Task status request received status code {0}:\n{1}'.format(status_code, pprint.pformat(r, indent=2)))
