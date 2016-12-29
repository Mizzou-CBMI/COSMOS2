import os
import itertools as it
import shutil
import codecs
import subprocess as sp
from sqlalchemy.orm import relationship, synonym, backref
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.schema import Column, ForeignKey, UniqueConstraint
from sqlalchemy.types import Boolean, Integer, String, DateTime, BigInteger
from flask import url_for
from networkx.algorithms import breadth_first_search

from ..db import Base
from ..util.sqla import Enum34_ColumnType, MutableDict, JSONEncodedDict, ListOfStrings, MutableList
from .. import TaskStatus, StageStatus, signal_task_status_change
from ..util.helpers import wait_for_file
import datetime
import pprint

opj = os.path.join


class ExpectedError(Exception): pass


class ToolError(Exception): pass


class ToolValidationError(Exception): pass


class GetOutputError(Exception): pass


task_failed_printout = u"""Failure Info:
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


@signal_task_status_change.connect
def task_status_changed(task):
    if task.status in [TaskStatus.successful]:
        if not task.NOOP:
            task.log.info('%s %s' % (task, task.status))

    if task.status == TaskStatus.waiting:
        task.started_on = datetime.datetime.now()

    elif task.status == TaskStatus.submitted:
        task.stage.status = StageStatus.running
        if not task.NOOP:
            task.log.info('%s %s. drm=%s; drm_jobid=%s' % (task, task.status, repr(task.drm), repr(task.drm_jobID)))
        task.submitted_on = datetime.datetime.now()

    elif task.status == TaskStatus.failed:
        if not task.must_succeed:
            task.log.warn('%s failed, but must_succeed is False' % task)
            task.log.warn(task_failed_printout.format(task))
            task.finished_on = datetime.datetime.now()
        else:
            task.log.warn('%s attempt #%s failed (max_attempts=%s)' % (task, task.attempt, task.workflow.max_attempts))
            if task.attempt < task.workflow.max_attempts:
                task.log.warn(task_failed_printout.format(task))
                task.attempt += 1
                task.status = TaskStatus.no_attempt
            else:
                wait_for_file(task.workflow, task.output_stderr_path, 60)

                task.log.warn(task_failed_printout.format(task))
                task.log.error('%s has failed too many times' % task)
                task.finished_on = datetime.datetime.now()
                task.stage.status = StageStatus.running_but_failed
                # task.session.commit()

    elif task.status == TaskStatus.successful:
        task.successful = True
        task.finished_on = datetime.datetime.now()
        if all(t.successful or not t.must_succeed for t in task.stage.tasks):
            task.stage.status = StageStatus.successful

            # task.session.commit()


# task_edge_table = Table('task_edge', Base.metadata,
# Column('parent_id', Integer, ForeignKey('task.id'), primary_key=True),
# Column('child_id', Integer, ForeignKey('task.id'), primary_key=True))



def logplus(filename):
    prefix, suffix = os.path.splitext(filename)
    return property(lambda self: opj(self.log_dir, "{0}_attempt{1}{2}".format(prefix, self.attempt, suffix)))


def readfile(path):
    if not os.path.exists(path):
        return 'file does not exist'

    try:
        with codecs.open(path, "r", "utf-8") as fh:
            s = fh.read(2 ** 20)
            if len(s) == 2**20:
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

    id = Column(Integer, primary_key=True)
    uid = Column(String(255), index=True)

    mem_req = Column(Integer, default=None)
    core_req = Column(Integer, default=1)
    cpu_req = synonym('core_req')
    time_req = Column(Integer)
    NOOP = Column(Boolean, default=False, nullable=False)
    params = Column(MutableDict.as_mutable(JSONEncodedDict), nullable=False, server_default='{}')
    stage_id = Column(ForeignKey('stage.id', ondelete="CASCADE"), nullable=False, index=True)
    log_dir = Column(String(255))
    # output_dir = Column(String(255))
    _status = Column(Enum34_ColumnType(TaskStatus), default=TaskStatus.no_attempt)
    successful = Column(Boolean, default=False, nullable=False)
    started_on = Column(DateTime)  # FIXME this should probably be deleted.  Too hard to determine.
    submitted_on = Column(DateTime)
    finished_on = Column(DateTime)
    attempt = Column(Integer, default=1)
    must_succeed = Column(Boolean, default=True)
    drm = Column(String(255), nullable=False)
    queue = Column(String(255))
    parents = relationship("Task",
                           secondary=TaskEdge.__table__,
                           primaryjoin=id == TaskEdge.parent_id,
                           secondaryjoin=id == TaskEdge.child_id,
                           backref="children",
                           passive_deletes=True,
                           cascade="save-update, merge, delete",
                           )

    input_map = Column(MutableDict.as_mutable(JSONEncodedDict), nullable=False, server_default='{}')
    output_map = Column(MutableDict.as_mutable(JSONEncodedDict), nullable=False, server_default='{}')

    @property
    def input_files(self):
        return self.input_map.values()

    @property
    def output_files(self):
        return self.output_map.values()

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

    extra = Column(MutableDict.as_mutable(JSONEncodedDict), nullable=False, server_default='{}')

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
        return self.status in [TaskStatus.successful, TaskStatus.failed]

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

    def all_predecessors(self, as_dict=False):
        """
        :return: (list) all tasks that descend from this task in the task_graph
        """
        d = breadth_first_search.bfs_predecessors(self.workflow.task_graph().reverse(copy=False), self)
        if as_dict:
            return d
        return set(d.values())

    def all_successors(self):
        """
        :return: (list) all tasks that descend from this task in the task_graph
        """
        return set(breadth_first_search.bfs_successors(self.workflow.task_graph(), self).values())

    @property
    def label(self):
        """Label used for the taskgraph image"""
        params = '' if len(self.params) == 0 else "\\n {0}".format(
                "\\n".join(["{0}: {1}".format(k, v) for k, v in self.params.items()]))

        return "[%s] %s%s" % (self.id, self.stage.name, params)

    def args_as_query_string(self):
        import urllib

        return urllib.urlencode(self.params)

    def delete(self, delete_files=False):
        self.log.debug('Deleting %s' % self)
        if delete_files:
            for tf in self.output_files:
                os.unlink(tf)
            if os.path.exists(self.log_dir):
                shutil.rmtree(self.log_dir)

        self.session.delete(self)
        self.session.commit()

    @property
    def url(self):
        return url_for('cosmos.task', ex_name=self.workflow.name, stage_name=self.stage.name, task_id=self.id)

    @property
    def params_pretty(self):
        return '%s' % ', '.join('%s=%s' % (k, "'%s'" % v if isinstance(v, basestring) else v) for k, v in self.params.items())

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
