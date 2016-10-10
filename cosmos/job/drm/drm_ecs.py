import pprint
import subprocess as sp
import re
import os
from collections import OrderedDict
import time
from .util import div, convert_size_to_kb
from cosmos.util.iterstuff import only_one
from more_itertools import grouper
from .DRM_Base import DRM

os.environ.setdefault('AWS_DEFAULT_REGION', 'us-east-1')


class DRM_ECS(DRM):
    name = 'efs'
    poll_interval = 5

    def __init__(self, *args, **kwargs):
        import boto3
        self.ecs = boto3.client('ecs')
        super(DRM_ECS, self).__init__(*args, **kwargs)
        self.drm_config = dict(cluster='pipe-dev',
                               container_name='pipe-dev:PIPE-2139-docker_v9',
                               taskDefinition='pipe-job2',
                               startedBy='cosmos')

    def submit_job(self, task):
        for p in [task.output_stdout_path, task.output_stderr_path]:
            if os.path.exists(p):
                os.unlink(p)

        r = self.ecs.run_task(cluster=self.drm_options['cluster'],
                              taskDefinition='%s:%s' % (self.drm_options['taskDefinition'], task.cpu_req),
                              startedBy=self.drm_options['startedBy'],
                              overrides=dict(containerOverrides=[dict(name=self.drm_options['container_name'],
                                                                      command=['/bin/bash',
                                                                               '-c', 'cd %s; %s' % (os.getcwd(), task.output_command_script_path),
                                                                               ])]))
        drm_jobID = r['tasks'][0]['taskArn']
        # ns = ' ' + task.drm_native_specification if task.drm_native_specification else ''
        return drm_jobID

    def filter_is_done(self, tasks):
        """
        """
        for task, task_response in self._describe_tasks(tasks).iteritems():
            ecs_status = task_response['lastStatus']
            if ecs_status == 'STOPPED':
                yield task, {
                    'exit_status': only_one(task_response['containers'])['exitCode'],
                }

    def kill(self, task):
        "Terminates a task"
        raise NotImplementedError

    def kill_tasks(self, tasks):
        for task in tasks:
            r = self.ecs.stop_task(
                cluster=self.drm_options['cluster'],
                task=task.drm_jobID,
                reason='killed by cosmos'
            )

            _check_status(r)

    def _describe_tasks(self, tasks):
        r = self.ecs.describe_tasks(cluster=self.drm_options['cluster'], tasks=[task.drm_jobID for task in tasks])

        # Error checking
        _check_status(r)

        return {t: task_r for t, task_r in zip(tasks, r['tasks'])}

    def drm_statuses(self, tasks):
        """
        Retrieves task statuses from ECS API
        :return: dict of {task: ecs_status} where status is in {RUNNING, PENDING, STOPPED}
        """
        return {t: task_r['lastStatus'] for t, task_r in self._describe_tasks(tasks)}


def _check_status(r):
    if 'failures' in r and len(r['failures']):
        raise Exception('Failures:\n{0}'.format(pprint.pformat(r['failures'], indent=2)))

    status_code = r['ResponseMetadata']['HTTPStatusCode']
    if status_code != 200:
        raise Exception('Task status request received status code {0}:\n{1}'.format(status_code, pprint.pformat(r, indent=2)))
