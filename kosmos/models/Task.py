import itertools as it
from inspect import getargspec, getcallargs
import os
import json
import re
from sqlalchemy import Column, Boolean, Integer, String, PickleType, ForeignKey, DateTime, func, Table,\
    UniqueConstraint, event
from sqlalchemy.orm import Session, backref
from sqlalchemy.orm import relationship, synonym
from sqlalchemy.ext.declarative import declared_attr
from flask import url_for

from ..util.helpers import parse_cmd, kosmos_format, groupby
from .TaskFile import TaskFile
from ..db import Base
from ..util.sqla import Enum34_ColumnType
from .. import TaskStatus, StageStatus, signal_task_status_change


opj = os.path.join


class ExpectedError(Exception): pass


class ToolError(Exception): pass


class ToolValidationError(Exception): pass


class GetOutputError(Exception): pass


@signal_task_status_change.connect
def task_status_changed(task):
    task.log.info('%s %s' % (task, task.status))
    if task.status == TaskStatus.waiting:
        task.started_on = func.now()

    elif task.status == TaskStatus.submitted:
        task.submitted_on = func.now()
        if task.stage.status == StageStatus.no_attempt:
            task.stage.status = StageStatus.running

    elif task.status == TaskStatus.failed:
        task.finished_on = func.now()
        if task.attempt < 3:
            task.log.warn('%s attempt %s failed, retrying' % (task, task.attempt))
            task.attempt += 1
            task.status = TaskStatus.no_attempt
        else:
            task.log.error('%s failed %s times' % (task, task.attempt))
            task.execution.terminate()

    elif task.status == TaskStatus.successful:
        task.successful = True
        task.finished_on = func.now()
        if all(t.successful for t in task.stage.tasks):
            task.stage.status = StageStatus.finished

    if task.status in [TaskStatus.successful, TaskStatus.failed]:
        for k, v in task.profile.items():
            setattr(task, k, v)

    task.session.commit()


task_edge_table = Table('task_edge', Base.metadata,
                        Column('parent_id', Integer, ForeignKey('task.id', ondelete='cascade'), primary_key=True),
                        Column('child_id', Integer, ForeignKey('task.id', ondelete='cascade'), primary_key=True)
)


# @event.listens_for(Session, 'after_flush')
# def delete_tag_orphans(session, ctx):
#     session.query(Task). \
#         filter(~Task.children.any()). \
#         delete(synchronize_session=False)


def logplus(p):
    return property(lambda self: opj(self.log_dir, p))


class Task(Base):
    """
    A Tool is a class who's instances represent a command that gets executed.  It also contains properties which
    define the resources that are required.

    :property stage: (str) The Tool's Stage
    :property dag: (TaskGraph) The dag that is keeping track of this Tool
    :property id: (int) A unique identifier.  Useful for debugging.
    :property input_files: (list) This Tool's input TaskFiles
    :property taskfiles: (list) This Tool's output TaskFiles.  A task's output taskfile names should be unique.
    :property tags: (dict) This Tool's tags.
    """
    __tablename__ = 'task'
    __table_args__ = (UniqueConstraint('tags', 'stage_id', name='_uc1'),)

    id = Column(Integer, primary_key=True)
    class_name = Column(String)
    mem_req = Column(Integer)
    cpu_req = Column(Integer)
    time_req = Column(Integer)
    NOOP = Column(Boolean, default=False, nullable=False)
    tags = Column(PickleType, nullable=False)
    stage_id = Column(ForeignKey('stage.id'), nullable=False)
    stage = relationship("Stage", backref="tasks")
    log_dir = Column(String)
    output_dir = Column(String)
    _status = Column(Enum34_ColumnType(TaskStatus), default=TaskStatus.no_attempt)
    successful = Column(Boolean, default=False, nullable=False)
    started_on = Column(DateTime)
    submitted_on = Column(DateTime)
    finished_on = Column(DateTime)
    attempt = Column(Integer, default=1)
    parents = relationship("Task",
                           secondary=task_edge_table,
                           primaryjoin=id == task_edge_table.c.parent_id,
                           secondaryjoin=id == task_edge_table.c.child_id,
                           backref='children',
                           cascade='all'
    )

    #drmaa related input fields
    drmaa_native_specification = Column(String)

    #drmaa related and job output fields
    drmaa_jobID = Column(Integer) #drmaa drmaa_jobID, note: not database primary key

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
    names = Column(String)
    num_processes = Column(Integer)
    pids = Column(String)
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

    @declared_attr
    def __mapper_args__(cls):
        return {
            "polymorphic_on": 'class_name',
            "polymorphic_identity": cls.__name__
        }

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


    def __init__(self, *args, **kwargs):
        """
        :param tags: (dict) A dictionary of tags.
        :param stage: (str) The stage this task belongs to.
        """
        #if len(tags)==0: raise ToolValidationError('Empty tag dictionary.  All tasks should have at least one tag.')
        super(Task, self).__init__(*args, **kwargs)

        self.settings = {}
        self.parameters = {}
        if not hasattr(self, 'inputs'): self.inputs = []
        if not hasattr(self, 'outputs'): self.outputs = []

        self.tags = {k: str(v) for k, v in self.tags.items()}

        # Because defining attributes in python creates a reference to a single instance across all class instance
        # any taskfile instances in self.outputs is used as a template for instantiating a new class
        # Create empty output TaskFiles
        for output in self.outputs:
            if isinstance(output, tuple):
                tf = TaskFile(name=output[0], basename=output[1])
            elif isinstance(output, str):
                tf = TaskFile(name=output)
            else:
                raise ToolValidationError, "{0}.outputs must be a list of strs or tuples.".format(self)
            self.taskfiles.append(tf)

        self._validate()

    def get_output(self, name, error_if_missing=True):
        for o in self.taskfiles:
            if o.name == name:
                return o

        if error_if_missing:
            raise ToolError, 'Output named `{0}` does not exist in {1}'.format(name, self)

    @property
    def input_files(self):
        "A list of input TaskFiles"
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

    @property
    def label(self):
        "Label used for the graphviz image"
        tags = '' if len(self.tags) == 0 else "\\n {0}".format(
            "\\n".join(["{0}: {1}".format(k, v) for k, v in self.tags.items()]))

        return "[%s] %s%s" % ( self.id, self.stage.name, tags)

    def map_inputs(self):
        """
        Default method to map inputs.  Can be overriden if a different behavior is desired
        :returns: (dict) A dictionary of taskfiles which are inputs to this task.  Keys are names of the taskfiles, values are a list of taskfiles.
        """
        if not self.inputs:
            return {}

        else:
            if '*' in self.inputs:
                return {'*': [o for p in self.parents for o in p.taskfiles]}

            all_inputs = filter(lambda x: x is not None,
                                [p.get_output(name, error_if_missing=False) for p in self.parents for name in
                                 self.inputs])

            input_dict = dict(
                (name, list(input_files)) for name, input_files in groupby(all_inputs, lambda i: i.name))

            for k in self.inputs:
                if k not in input_dict or len(input_dict[k]) == 0:
                    raise ToolValidationError, "Could not find input '{0}' for {1}".format(k, self)

            return input_dict


    def generate_cmd(self):
        """
        Calls map_inputs() and processes the output of cmd()
        """
        p = self.parameters.copy()
        argspec = getargspec(self.cmd)

        for k, v in self.tags.items():
            if k in argspec.args:
                p[k] = v

        ## Validation
        # Helpful error message
        if not argspec.keywords: #keywords is the **kwargs name or None if not specified
            for k in p.keys():
                if k not in argspec.args:
                    raise ToolValidationError, '{0} received the parameter "{1}" which is not defined in it\'s signature.  Parameters are {2}.  Accept the parameter **kwargs in cmd() to generalize the parameters accepted.'.format(
                        self, k, argspec.args)

        for l in ['i', 'o', 's']:
            if l in p.keys():
                raise ToolValidationError, "{0} is a reserved name, and cannot be used as a tag keyword".format(l)

        try:
            kwargs = dict(i=self.map_inputs(), o={o.name: o for o in self.taskfiles}, s=self.settings, **p)
            callargs = getcallargs(self.cmd, **kwargs)
        except TypeError:
            raise TypeError, 'Invalid parameters for {0}.cmd(): {1}'.format(self, kwargs.keys())

        del callargs['self']
        r = self.cmd(**callargs)

        #if tuple is returned, second element is a dict to format with
        extra_format_dict = r[1] if len(r) == 2 and r else {}
        pcmd = r[0] if len(r) == 2 else r

        #format() return string with callargs
        callargs['self'] = self
        callargs.update(extra_format_dict)
        cmd = kosmos_format(pcmd, callargs)

        #fix TaskFiles paths
        cmd = re.sub('<TaskFile\[\d+?\] (.+?)>', lambda x: x.group(1), cmd)

        return parse_cmd(cmd)


    def cmd(self, i, o, s, **kwargs):
        """
        Constructs the preformatted command string.  The string will be .format()ed with the i,s,p dictionaries,
        and later, $OUT.outname  will be replaced with a TaskFile associated with the output name `outname`

        :param i: (dict) Input TaskFiles.
        :param o: (dict) Input TaskFiles.
        :param s: (dict) Settings.
        :param kwargs: (dict) Parameters.
        :returns: (str|tuple(str,dict)) A preformatted command string, or a tuple of the former and a dict with extra values to use for
            formatting
        """
        raise NotImplementedError("{0}.cmd is not implemented.".format(self.__class__.__name__))


    def configure(self, settings={}, parameters={}):
        """
        """
        self.parameters = parameters
        self.settings = settings
        return self


    def _validate(self):
        #validate inputs are strs
        if any([not isinstance(i, str) for i in self.inputs]):
            raise ToolValidationError, "{0} has elements in self.inputs that are not of type str".format(self)

        if len(self.inputs) != len(set(self.inputs)):
            raise ToolValidationError(
                'Duplicate names in task.inputs detected in {0}.  Perhaps try using [1.ext,2.ext,...]'.format(self))

        output_names = [o.name for o in self.taskfiles]
        if len(output_names) != len(set(output_names)):
            raise ToolValidationError(
                'Duplicate names in task.taskfiles detected in {0}.  Perhaps try using [1.ext,2.ext,...] when defining outputs'.format(
                    self))

    @property
    def url(self):
        return url_for('.task', id=self.id)

    def __repr__(self):
        s = self.stage.name if self.stage else ''
        return '<Task[%s] %s %s>' % (self.id or 'id_%s' % id(self), s, self.tags)


class INPUT(Task):
    """
    An Input File.

    Does not actually execute anything, but provides a way to load an input file.

    >>> INPUT('/path/to/file.ext',tags={'key':'val'})
    >>> INPUT(path='/path/to/file.ext.gz',name='ext',fmt='ext.gz',tags={'key':'val'})
    """

    def __init__(self, path, tags, name=None, fmt=None, *args, **kwargs):
        """
        :param path: the path to the input file
        :param name: the name or keyword for the input file
        :param fmt: the format of the input file
        """
        path = os.path.abspath(path)
        super(INPUT, self).__init__(tags=tags, *args, **kwargs)
        self.NOOP = True
        self.persist = True
        self.taskfiles.append(TaskFile(path=path, name=name, task=self))

        # def __str__(self):
        #     return '[{0}] {1} {2}'.format(self.id, self.__class__.__name__, self.tags)