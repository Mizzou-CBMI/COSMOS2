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

        for k in ('cluster', 'container_image', 'task_family', 'mounts', 'started_by'):
            assert k in self.drm_options, '%s must be set in the ECS drm_options' % k

        self.task_id_to_task_definition_arn = dict()

    def submit_job(self, task):
        for p in [task.output_stdout_path, task.output_stderr_path]:
            if os.path.exists(p):
                os.unlink(p)

        r = self.ecs.register_task_definition(
            **{u'containerDefinitions': [{u'cpu': task.core_req * 1024,
                                          u'command': ['/bin/bash',
                                                       '-c', 'cd %s; %s' % (os.getcwd(), task.output_command_script_path),
                                                       ],
                                          u'environment': [],
                                          u'essential': True,
                                          u'image': self.drm_options['container_image'],
                                          u'memoryReservation': task.mem_req or 1000,
                                          u'mountPoints': [m[''] for m in self.drm_options['mounts']],
                                          u'name': task.stage.name,
                                          u'portMappings': [],
                                          u'readonlyRootFilesystem': False,
                                          u'volumesFrom': []}],
               u'family': self.drm_options['task_family'],
               u'networkMode': u'bridge',
               u'volumes': [{u'host': {u'sourcePath': m['sourcePath']}, u'name': m['name']} for m in self.drm_options['mounts']]}
            # u'volumes': [{u'host': {u'sourcePath': u'/locus'}, u'name': u'efs'}]}
        )

        _check_response_for_error(r)
        self.task_id_to_task_definition_arn[task.id] = r['taskDefinition']['taskDefinitionArn']

        r = self.ecs.run_task(cluster=self.drm_options['cluster'],
                              taskDefinition=r['taskDefinition']['taskDefinitionArn'],
                              startedBy=self.drm_options['started_by'])
        _check_response_for_error(r)

        drm_jobID = only_one(r['tasks'])['taskArn']
        # ns = ' ' + task.drm_native_specification if task.drm_native_specification else ''
        return drm_jobID

    def clean_up(self, task):
        if task.id in self.task_id_to_task_definition_arn:
            self.ecs.deregister_task_definition(taskDefinition=self.task_id_to_task_definition_arn.pop(task.id))

    def filter_is_done(self, tasks):
        """
        """
        for task, task_response in self._describe_tasks(tasks).iteritems():
            ecs_status = task_response['lastStatus']

            if ecs_status == 'STOPPED':
                if 'exitCode' not in only_one(task_response['containers']):
                    raise Exception('Task did does not have a valid exit code: %s' % pprint.pformat(task_response, indent=2))

                yield task, {
                    'exit_status': only_one(task_response['containers'])['exitCode'],
                }

    def kill(self, task):
        "Terminates a task"
        raise NotImplementedError

    def kill_tasks(self, tasks):
        for task in tasks:
            if task.drm_jobID is not None:
                r = self.ecs.stop_task(
                    cluster=self.drm_options['cluster'],
                    task=task.drm_jobID,
                    reason='killed by cosmos'
                )
            _check_response_for_error(r)
            self.clean_up(task)

    def _describe_tasks(self, tasks):
        r = self.ecs.describe_tasks(cluster=self.drm_options['cluster'], tasks=[task.drm_jobID for task in tasks])

        # Error checking
        _check_response_for_error(r)

        return {t: task_r for t, task_r in zip(tasks, r['tasks'])}

    def drm_statuses(self, tasks):
        """
        Retrieves task statuses from ECS API
        :return: dict of {task: ecs_status} where status is in {RUNNING, PENDING, STOPPED}
        """
        return {t: task_r['lastStatus'] for t, task_r in self._describe_tasks(tasks)}


def _check_response_for_error(r):
    if 'failures' in r and len(r['failures']):
        raise Exception('Failures:\n{0}'.format(pprint.pformat(r['failures'], indent=2)))

    status_code = r['ResponseMetadata']['HTTPStatusCode']
    if status_code != 200:
        raise Exception('Task status request received status code {0}:\n{1}'.format(status_code, pprint.pformat(r, indent=2)))
