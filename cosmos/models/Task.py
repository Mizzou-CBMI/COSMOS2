import codecs
import datetime
import os
import pprint
import subprocess as sp

import networkx as nx
from flask import url_for
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.ext.declarative.base import _declarative_constructor
from sqlalchemy.orm import reconstructor, relationship, synonym
from sqlalchemy.schema import Column, ForeignKey, UniqueConstraint
from sqlalchemy.types import Boolean, DateTime, Integer, String

from cosmos import StageStatus, TaskStatus, signal_task_status_change
from cosmos.db import Base
from cosmos.util.helpers import wait_for_file
from cosmos.util.sqla import Enum_ColumnType, JSONEncodedDict, MutableDict


class ExpectedError(Exception): pass


class ToolError(Exception): pass


class ToolValidationError(Exception): pass


class GetOutputError(Exception): pass


task_printout = u"""Task Info:
<EXIT_STATUS="{0.exit_status}">
<COMMAND path="{0.output_command_script_path}" drm_jobID="{0.drm_jobID}">
<PARAMS>
{0.params_pformat}
</PARAMS>
{0.command_script_text}
</COMMAND>
<STDOUT path="{0.output_stdout_path}">
{0.stdout_text}
</STDOUT>
<STDERR path="{0.output_stderr_path}">
{0.stderr_text}
</STDERR>
"""

completed_task_statuses = {TaskStatus.failed, TaskStatus.killed, TaskStatus.successful}


@signal_task_status_change.connect
def task_status_changed(task):
    if task.status in completed_task_statuses:
        task.workflow.jobmanager.get_drm(task.drm).populate_logs(task)

    if task.status == TaskStatus.waiting:
        task.started_on = datetime.datetime.now()

    elif task.status == TaskStatus.submitted:
        task.stage.status = StageStatus.running
        if not task.NOOP:
            task.log.info(
                '%s %s. drm=%s; drm_jobid=%s; job_class=%s; queue=%s' %
                (task, task.status, repr(task.drm), repr(task.drm_jobID),
                 repr(task.job_class), repr(task.queue)))
        task.submitted_on = datetime.datetime.now()

    elif task.status == TaskStatus.failed:
        if not task.must_succeed:
            task.log.warn('%s failed, but must_succeed is False' % task)
            task.log.warn(task_printout.format(task))
            task.finished_on = datetime.datetime.now()
        else:
            #
            # By default /usr/bin/timeout returns 124 when it kills a job.
            # DRM_Local jobs that time out will usually have this error code.
            # Other DRM's may well have different error codes. Currently, this
            # check is purely cosmetic, but if we do more here, then
            # FIXME we should have a DRM-agnostic way of determining timed-out tasks.
            #
            if task.exit_status == 124:
                exit_reason = 'timed out'
            else:
                exit_reason = 'failed'

            task.log.warn('%s attempt #%s %s (max_attempts=%s)' % (task, task.attempt, exit_reason, task.max_attempts))

            if task.attempt < task.max_attempts:
                task.log.warn(task_printout.format(task))
                task.attempt += 1
                task.status = TaskStatus.no_attempt
            else:
                wait_for_file(task.workflow, task.output_stderr_path, 30, error=False)

                task.log.warn(task_printout.format(task))
                task.log.error('%s has failed too many times' % task)
                task.finished_on = datetime.datetime.now()
                task.stage.status = StageStatus.running_but_failed

    elif task.status == TaskStatus.successful:
        task.successful = True
        if not task.NOOP:
            task.log.info('{} {}, wall_time: {}.  {}/{} Tasks finished.'.format(task, task.status,
                                                                                datetime.timedelta(
                                                                                    seconds=task.wall_time),
                                                                                sum(1 for t in task.workflow.tasks if
                                                                                    t.finished),
                                                                                len(task.workflow.tasks)))
        task.finished_on = datetime.datetime.now()
        if all(t.successful or not t.must_succeed for t in task.stage.tasks):
            task.stage.status = StageStatus.successful


# task_edge_table = Table('task_edge', Base.metadata,
# Column('parent_id', Integer, ForeignKey('task.id'), primary_key=True),
# Column('child_id', Integer, ForeignKey('task.id'), primary_key=True))


def logplus(filename):
    prefix, suffix = os.path.splitext(filename)
    return property(lambda self: os.path.join(
        self.log_dir, "{0}_attempt{1}{2}".format(prefix, self.attempt, suffix)))


def readfile(path):
    if not os.path.exists(path):
        return '%s file does not exist!' % path

    try:
        with codecs.open(path, "r", "utf-8") as fh:
            s = fh.read(2 ** 20)
            if len(s) == 2 ** 20:
                s += '\n*****TRUNCATED, check log file for full output*****'
            return s
    except:
        return 'error parsing as utf-8: %s' % path


class TaskEdge(Base):
    __tablename__ = 'task_edge'
    # id = Column(Integer, primary_key=True)
    parent_id = Column(Integer, ForeignKey('task.id', ondelete="CASCADE"), primary_key=True)
    child_id = Column(Integer, ForeignKey('task.id', ondelete="CASCADE"), primary_key=True)

    def __init__(self, parent=None, child=None):
        self.parent = parent
        self.child = child

    def __str__(self):
        return '<TaskEdge: %s -> %s>' % (self.parent, self.child)

    def __repr__(self):
        return self.__str__()


class Task(Base):
    __tablename__ = 'task'
    """
    A job that gets executed.  Has a unique set of params within its Stage.
    """
    # FIXME causes a problem with mysql?
    __table_args__ = (UniqueConstraint('stage_id', 'uid', name='_uc1'),)

    drm_options = {}

    id = Column(Integer, primary_key=True)
    uid = Column(String(255), index=True)

    mem_req = Column(Integer)
    core_req = Column(Integer)
    cpu_req = synonym('core_req')
    time_req = Column(Integer)
    NOOP = Column(Boolean, nullable=False)
    params = Column(MutableDict.as_mutable(JSONEncodedDict), nullable=False)
    stage_id = Column(ForeignKey('stage.id', ondelete="CASCADE"), nullable=False, index=True)
    log_dir = Column(String(255))
    # output_dir = Column(String(255))
    _status = Column(Enum_ColumnType(TaskStatus, length=255), default=TaskStatus.no_attempt, nullable=False)
    successful = Column(Boolean, nullable=False)
    started_on = Column(DateTime)  # FIXME this should probably be deleted.  Too hard to determine.
    submitted_on = Column(DateTime)
    finished_on = Column(DateTime)
    attempt = Column(Integer, nullable=False)
    must_succeed = Column(Boolean, nullable=False)
    drm = Column(String(255))
    # FIXME consider making job_class a proper field next time the schema changes
    # job_class = Column(String(255))
    queue = Column(String(255))
    max_attempts = Column(Integer)
    parents = relationship("Task",
                           secondary=TaskEdge.__table__,
                           primaryjoin=id == TaskEdge.parent_id,
                           secondaryjoin=id == TaskEdge.child_id,
                           backref="children",
                           passive_deletes=True,
                           cascade="save-update, merge, delete",
                           )

    # input_map = Column(MutableDict.as_mutable(JSONEncodedDict), nullable=False)
    # output_map = Column(MutableDict.as_mutable(JSONEncodedDict), nullable=False)

    @property
    def input_map(self):
        d = dict()
        for key, val in self.params.items():
            if key.startswith('in_'):
                d[key] = val
        return d

    @property
    def output_map(self):
        d = dict()
        for key, val in self.params.items():
            if key.startswith('out_'):
                d[key] = val
        return d

    @property
    def input_files(self):
        return list(self.input_map.values())

    @property
    def output_files(self):
        return list(self.output_map.values())

    # command = Column(Text)

    drm_native_specification = Column(String(255))
    drm_jobID = Column(String(255))

    profile_fields = ['wall_time', 'cpu_time', 'percent_cpu', 'user_time', 'system_time', 'io_read_count',
                      'io_write_count', 'io_read_kb', 'io_write_kb',
                      'ctx_switch_voluntary', 'ctx_switch_involuntary', 'avg_rss_mem_kb', 'max_rss_mem_kb',
                      'avg_vms_mem_kb', 'max_vms_mem_kb', 'avg_num_threads',
                      'max_num_threads',
                      'avg_num_fds', 'max_num_fds', 'exit_status']
    exclude_from_dict = profile_fields + ['command', 'info', 'input_files', 'output_files']

    exit_status = Column(Integer)

    percent_cpu = Column(Integer)
    wall_time = Column(Integer)

    cpu_time = Column(Integer)
    user_time = Column(Integer)
    system_time = Column(Integer)

    avg_rss_mem_kb = Column(Integer)
    max_rss_mem_kb = Column(Integer)
    avg_vms_mem_kb = Column(Integer)
    max_vms_mem_kb = Column(Integer)

    io_read_count = Column(Integer)
    io_write_count = Column(Integer)
    io_wait = Column(Integer)
    io_read_kb = Column(Integer)
    io_write_kb = Column(Integer)

    ctx_switch_voluntary = Column(Integer)
    ctx_switch_involuntary = Column(Integer)

    avg_num_threads = Column(Integer)
    max_num_threads = Column(Integer)

    avg_num_fds = Column(Integer)
    max_num_fds = Column(Integer)

    extra = Column(MutableDict.as_mutable(JSONEncodedDict), nullable=False)

    @declared_attr
    def status(cls):
        def get_status(self):
            return self._status

        def set_status(self, value):
            if self._status != value:
                self._status = value
                signal_task_status_change.send(self)

        return synonym('_status', descriptor=property(get_status, set_status))

    @property
    def workflow(self):
        return self.stage.workflow

    @property
    def log(self):
        return self.workflow.log

    @property
    def finished(self):
        return self.status in {TaskStatus.successful, TaskStatus.killed, TaskStatus.failed}

    _cache_profile = None

    output_profile_path = logplus('profile.json')
    output_command_script_path = logplus('command.bash')
    output_stderr_path = logplus('stderr.txt')
    output_stdout_path = logplus('stdout.txt')

    @property
    def stdout_text(self):
        return readfile(self.output_stdout_path)

    @property
    def stderr_text(self):
        r = readfile(self.output_stderr_path)
        if r == 'file does not exist':
            if self.drm == 'lsf' and self.drm_jobID:
                r += '\n\nbpeek %s output:\n\n' % self.drm_jobID
                try:
                    r += codecs.decode(sp.check_output('bpeek %s' % self.drm_jobID, shell=True), 'utf-8')
                except Exception as e:
                    r += str(e)
        return r

    @property
    def command_script_text(self):
        # return self.command
        return readfile(self.output_command_script_path).strip() or self.command

    def descendants(self, include_self=False):
        """
        :return: (list) all stages that descend from this stage in the stage_graph
        """
        x = nx.descendants(self.workflow.task_graph(), self)
        if include_self:
            return sorted({self}.union(x), key=lambda task: task.stage.number)
        else:
            return x

    @property
    def label(self):
        """Label used for the taskgraph image"""
        params = '' if len(self.params) == 0 else "\\n {0}".format(
            "\\n".join(["{0}: {1}".format(k, v) for k, v in self.params.items()]))

        return "[%s] %s%s" % (self.id, self.stage.name, params)

    def args_as_query_string(self):
        import urllib

        return urllib.urlencode(self.params)

    def delete(self, descendants=False):
        if descendants:
            tasks_to_delete = self.descendants(include_self=True)
            self.log.debug('Deleting %s and %s of its descendants' % (self, len(tasks_to_delete) - 1))
            for t in tasks_to_delete:
                self.session.delete(t)
        else:
            self.log.debug('Deleting %s' % self)
            self.session.delete(self)

        self.session.commit()

    @property
    def url(self):
        return url_for('cosmos.task', ex_name=self.workflow.name, stage_name=self.stage.name, task_id=self.id)

    @property
    def params_pretty(self):
        return '%s' % ', '.join(
            '%s=%s' % (k, "'%s'" % v if isinstance(v, basestring) else v) for k, v in self.params.items())

    @property
    def params_pformat(self):
        return pprint.pformat(self.params, indent=2, width=1)

    def __repr__(self):
        return "<Task[%s] %s(uid='%s')>" % (self.id or 'id_%s' % id(self),
                                            self.stage.name if self.stage else '',
                                            self.uid
                                            )

    def __str__(self):
        return self.__repr__()

    # FIXME consider making job_class a proper field next time the schema changes
    def __init__(self, **kwargs):
        self.job_class = kwargs.pop('job_class', None)
        _declarative_constructor(self, **kwargs)

    @reconstructor
    def init_on_load(self):
        self.job_class = None
