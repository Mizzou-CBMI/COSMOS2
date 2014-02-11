import itertools as it
import os
import json
from sqlalchemy import Column, Boolean, Integer, String, PickleType, ForeignKey, DateTime, func, Table, \
    UniqueConstraint, Text
from sqlalchemy.orm import relationship, synonym, backref
from sqlalchemy.ext.declarative import declared_attr
from flask import url_for

from ..db import Base
from ..util.sqla import Enum34_ColumnType, ListOfStrings
from .. import TaskStatus, StageStatus, signal_task_status_change
from .. import ExecutionFailed

opj = os.path.join


class ExpectedError(Exception): pass


class ToolError(Exception): pass


class ToolValidationError(Exception): pass


class GetOutputError(Exception): pass


task_failed_printout = """<command path="{task.output_command_script_path}">
{cmd}
</command>
<stderr>
{stderr}
</stderr>
<stdout>
{stdout}
</stdout>
"""


@signal_task_status_change.connect
def task_status_changed(task):
    if task.status in [TaskStatus.successful, TaskStatus.killed, TaskStatus.submitted]:
        task.log.info('%s %s' % (task, task.status))
        task.session.commit()

    if task.status == TaskStatus.waiting:
        task.started_on = func.now()

    elif task.status == TaskStatus.submitted:
        task.submitted_on = func.now()
        task.stage.status = StageStatus.running

    elif task.status == TaskStatus.failed:
        task.finished_on = func.now()
        msg = task_failed_printout.format(
            task=task,
            stdout=task.stdout_text,
            stderr=task.stderr_text,
            cmd=task.command_script_text
        )
        if not task.must_succeed:
            task.log.warn('%s failed, but must_succeed is False' % task)
            task.log.warn(msg)
        # elif task.attempt < 3:
        #     task.log.warn('%s attempt #%s failed' % (task, task.attempt))
        #     task.log.warn(msg)
        #     task.attempt += 1
        #     task.status = TaskStatus.no_attempt
        else:
            task.log.error('%s failed %s times' % (task, task.attempt))
            task.log.warn(msg)
            task.finished_on = func.now()
            task.stage.status = StageStatus.failed
            task.session.commit()
            task.update_from_profile_output()
            raise ExecutionFailed

    elif task.status == TaskStatus.successful:
        task.successful = True
        task.finished_on = func.now()
        if all(t.successful or not t.must_succeed for t in task.stage.tasks):
            task.stage.status = StageStatus.successful
        task.update_from_profile_output()


    task.session.commit()


task_edge_table = Table('task_edge', Base.metadata,
                        Column('parent_id', Integer, ForeignKey('task.id'), primary_key=True),
                        Column('child_id', Integer, ForeignKey('task.id'), primary_key=True)
)


def logplus(p):
    return property(lambda self: opj(self.log_dir, p))


def readfile(path):
    if not os.path.exists(path):
        return 'file does not exist'
    with open(path, 'r') as fh:
        return fh.read()


class Task(Base):
    __tablename__ = 'task'
    """
    A job that gets executed.  Has a unique set of tags within its Stage.
    """
    # causes a problem with mysql.  its checked a the application level so should be okay
    #__table_args__ = (UniqueConstraint('tags', 'stage_id', name='_uc1'),)

    id = Column(Integer, primary_key=True)
    mem_req = Column(Integer)
    cpu_req = Column(Integer, default=1, nullable=False)
    time_req = Column(Integer)
    NOOP = Column(Boolean, default=False, nullable=False)
    tags = Column(PickleType, nullable=False)
    stage_id = Column(ForeignKey('stage.id'), nullable=False)
    stage = relationship("Stage", backref=backref("tasks", cascade="all, delete-orphan"))
    log_dir = Column(String(255))
    output_dir = Column(String(255))
    _status = Column(Enum34_ColumnType(TaskStatus), default=TaskStatus.no_attempt)
    successful = Column(Boolean, default=False, nullable=False)
    started_on = Column(DateTime)
    submitted_on = Column(DateTime)
    finished_on = Column(DateTime)
    attempt = Column(Integer, default=1)
    must_succeed = Column(Boolean, default=True)
    parents = relationship("Task",
                           secondary=task_edge_table,
                           primaryjoin=id == task_edge_table.c.parent_id,
                           secondaryjoin=id == task_edge_table.c.child_id,
                           backref='children')
    command = Column(Text)
    forward_inputs = Column(ListOfStrings)

    #drmaa related input fields
    drmaa_native_specification = Column(String(255))

    #drmaa related and job output fields
    drmaa_jobID = Column(Integer)

    profile_fields = [('time', ['user_time', 'system_time', 'cpu_time', 'wall_time', 'percent_cpu']),
                      ('memory', ['avg_rss_mem', 'max_rss_mem', 'single_proc_max_peak_virtual_mem',
                                  'avg_virtual_mem', 'max_virtual_mem', 'single_proc_max_peak_rss',
                                  'minor_page_faults', 'major_page_faults',
                                  'avg_pte_mem', 'max_pte_mem',
                                  'avg_locked_mem', 'max_locked_mem',
                                  'avg_data_mem', 'max_data_mem',
                                  'avg_lib_mem', 'max_lib_mem']),
                      ('i/o', ['voluntary_context_switches', 'nonvoluntary_context_switches', 'block_io_delays',
                               'avg_fdsize', 'max_fdsize']),
                      ('misc', ['exit_status', 'names', 'pids', 'num_polls', 'num_processes', 'SC_CLK_TCK',
                                'avg_num_threads', 'max_num_threads'])
    ]

    exclude_from_dict = [field for cat, fields in profile_fields for field in fields] + ['command','info']

    #time
    system_time = Column(Integer)
    user_time = Column(Integer)
    cpu_time = Column(Integer)
    wall_time = Column(Integer)
    percent_cpu = Column(Integer)

    #memory
    avg_rss_mem = Column(Integer)
    max_rss_mem = Column(Integer)
    single_proc_max_peak_rss = Column(Integer)
    avg_virtual_mem = Column(Integer)
    max_virtual_mem = Column(Integer)
    single_proc_max_peak_virtual_mem = Column(Integer)
    major_page_faults = Column(Integer)
    minor_page_faults = Column(Integer)
    avg_data_mem = Column(Integer)
    max_data_mem = Column(Integer)
    avg_lib_mem = Column(Integer)
    max_lib_mem = Column(Integer)
    avg_locked_mem = Column(Integer)
    max_locked_mem = Column(Integer)
    avg_num_threads = Column(Integer)
    max_num_threads = Column(Integer)
    avg_pte_mem = Column(Integer)
    max_pte_mem = Column(Integer)

    #io
    nonvoluntary_context_switches = Column(Integer)
    voluntary_context_switches = Column(Integer)
    block_io_delays = Column(Integer)
    avg_fdsize = Column(Integer)
    max_fdsize = Column(Integer)

    #misc
    num_polls = Column(Integer)
    names = Column(Text)
    num_processes = Column(Integer)
    pids = Column(Text)
    exit_status = Column(Integer)
    SC_CLK_TCK = Column(Integer)

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
    def execution(self):
        return self.stage.execution

    @property
    def log(self):
        return self.execution.log

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
        return readfile(self.output_stdout_path).strip()

    @property
    def stderr_text(self):
        return readfile(self.output_stderr_path).strip()

    @property
    def command_script_text(self):
        return readfile(self.output_command_script_path).strip()

    def get_output(self, name, error_if_missing=True):
        for o in self.output_files:
            if o.name == name:
                return o

        if name in self.forward_inputs:
            for p in self.parents:
                o = p.get_output(name, error_if_missing=False)
                if o:
                    return o

        if error_if_missing:
            raise ValueError('Output named `{0}` does not exist in {1}'.format(name, self))

    @property
    def input_files(self):
        """A list of input TaskFiles"""
        return list(it.chain(*[tf for tf in self.map_inputs().values()]))

    @property
    def profile(self):
        if self._cache_profile is None:
            if not os.path.exists(self.output_profile_path):
                return {}
            else:
                try:
                    with open(self.output_profile_path, 'r') as fh:
                        self._cache_profile = json.load(fh)
                except ValueError:
                    return {}
        return self._cache_profile

    def update_from_profile_output(self):
        for k, v in self.profile.items():
            setattr(self, k, v)

    @property
    def label(self):
        """Label used for the taskgraph image"""
        tags = '' if len(self.tags) == 0 else "\\n {0}".format(
            "\\n".join(["{0}: {1}".format(k, v) for k, v in self.tags.items()]))

        return "[%s] %s%s" % ( self.id, self.stage.name, tags)

    def tags_as_query_string(self):
        import urllib

        return urllib.urlencode(self.tags)

    def delete(self, delete_output_files=False):
        if delete_output_files:
            for tf in self.output_files:
                tf.delete()

        self.session.delete(self)
        self.session.commit()

    @property
    def url(self):
        return url_for('kosmos.task', id=self.id)

    def __repr__(self):
        s = self.stage.name if self.stage else ''
        return '<Task[%s] %s %s>' % (self.id or 'id_%s' % id(self), s, self.tags)