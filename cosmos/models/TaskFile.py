import shutil
import os
from sqlalchemy.schema import Column, ForeignKey, UniqueConstraint
from sqlalchemy.types import Boolean, Integer, String
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import relationship, backref
from collections import namedtuple

from ..db import Base


class TaskFileValidationError(Exception): pass


class TaskFileError(Exception): pass


# association_table = Table('input_files', Base.metadata,
# Column('task', Integer, ForeignKey('task.id')),
# Column('taskfile', Integer, ForeignKey('taskfile.id')))

AbstractInputFile = namedtuple('AbstractInputFile', ['name', 'format', 'forward', 'n'])
AbstractOutputFile = namedtuple('AbstractOutputFile', ['name', 'format', 'basename', 'persist'])


def abstract_input_taskfile(name='.*', format='.*', forward=False, n=1):
    """
    :param str name: A regular expression pattern to match the name of the TaskFile(s).
    :param str format: A regular expression pattern to match the format of the TaskFile(s).
    :param bool forward: Forward this input as an output of this Tool.
    :param int|str n: Cardinality.  examples: 1, >=1, <5, ==3.
    :rtype: AbstractInputFile
    """
    # assert name or format, 'must specify either name or format'

    return AbstractInputFile(name=name, format=format, forward=forward, n=n)


def abstract_output_taskfile_old(name=None, format=None, basename=None, persist=False):
    """
    :param name: (str) The name of the TaskFile.
    :param format: The format of the TaskFile.
    :param basename: (str) custom_name.custom_format  Defaults to name.format if not specified.
    :rtype: (AbstractOutputFile)
    """
    assert (name and format) or basename, 'must specify name, format or basename'
    if name is None:
        name, ext = os.path.splitext(os.path.basename(basename))
        name = name
        format = ext[1:]

    return AbstractOutputFile(name=name, format=format, basename=basename, persist=persist)


def abstract_output_taskfile(basename=None, name=None, format=None, persist=False):
    """
    :param str name: The name of the TaskFile.
    :param str format: The format of the TaskFile.
    :param str basename: custom_name.custom_format.  Defaults to name.format if not specified.
    :rtype: AbstractOutputFile
    """
    assert (name and format) or basename, 'must specify basename or both name and format'
    if basename:
        name2, ext = os.path.splitext(os.path.basename(basename))
        # if ext == 'gz':
        #     name2, ext2 = os.path.splitext(name2)
        #     ext = ext2 + '.' + ext

        if name is None:
            name = name2
        if format is None:
            format = ext[1:]

    return AbstractOutputFile(name=name, format=format, basename=basename, persist=persist)


class InputFileAssociation(Base):
    __tablename__ = 'input_file_assoc'
    forward = Column(Boolean, default=False)
    task_id = Column(Integer, ForeignKey('task.id', ondelete="CASCADE"), primary_key=True)
    taskfile_id = Column(Integer, ForeignKey('taskfile.id', ondelete="CASCADE"), primary_key=True)

    # def delete(self):
    # self.task._input_file_assocs.remove(self)
    # self.taskfile._input_file_assocs.remove(self)

    def __init__(self, taskfile=None, task=None, forward=False):
        assert not (taskfile is None and task is None)
        self.taskfile = taskfile
        self.task = task
        self.forward = forward


    def __repr__(self):
        return '<InputFileAssociation (%s) (%s)>' % (self.task, self.taskfile)

    def __str__(self):
        return self.__repr__()


class TaskFile(Base):
    """
    Task File.
    """
    __tablename__ = 'taskfile'
    __table_args__ = (UniqueConstraint('task_output_for_id', 'name', 'format', name='_uc_tf_name_fmt'),)

    id = Column(Integer, primary_key=True)
    task_output_for_id = Column(ForeignKey('task.id', ondelete="CASCADE"), index=True)
    order = Column(Integer, nullable=False)
    path = Column(String(255), nullable=False)
    name = Column(String(255), nullable=False)
    format = Column(String(255), nullable=False)
    basename = Column(String(255), nullable=False)  # todo basename redundant with path?
    persist = Column(Boolean, default=False)
    duplicate_ok = Column(Boolean, default=False)
    _input_file_assocs = relationship("InputFileAssociation", backref=backref("taskfile"), cascade="all, delete-orphan",
                                      passive_deletes=True)
    tasks_input_for = association_proxy('_input_file_assocs', 'task', creator=lambda t: InputFileAssociation(task=t))

    # @property
    # def basename(self):
    # return os.path.basename(self.path)

    # @property
    # def tasks_input_for(self):
    # return [ifa.task for ifa in self._input_file_assocs]

    @property
    def prefix(self):
        return self.basename.split('.')[0]

    @property
    def log(self):
        return self.task_output_for.log

    @property
    def execution(self):
        return self.task_output_for.execution

    def __init__(self, *args, **kwargs):
        super(TaskFile, self).__init__(*args, **kwargs)
        assert self.name is not None, 'TaskFile.name is required'
        assert self.format is not None, 'TaskFile.format is required'
        if self.basename is None:
            self.basename = '%s.%s' % (self.name, self.format) if self.format != 'dir' else self.name
        assert self.basename != '', 'basename is an empty string for %s' % self

    def __repr__(self):
        return '<TaskFile[%s] %s.%s:%s>' % (
            self.id or 'id_%s' % id(self), self.name, self.format, self.path or 'no_path_yet')

    def delete(self, delete_file=True):
        """
        Deletes this task and all files associated with it
        """
        self.log.debug('Deleting %s' % self)

        # if not self.task_output_for.NOOP and delete_file and os.path.exists(self.path):
        if not self.task_output_for.NOOP and delete_file and os.path.exists(self.path):
            if not in_directory(self.path, self.execution.output_dir):
                self.log.warn('Not deleting %s, outside of %s' % (self.path, self.execution.output_dir))
            else:
                if os.path.isdir(self.path):
                    shutil.rmtree(self.path)
                else:
                    os.remove(self.path)

        self.session.delete(self)
        # self.session.commit()


def in_directory(file, directory):
    # make both absolute
    directory = os.path.join(os.path.realpath(directory), '')
    file = os.path.realpath(file)

    # return true, if the common prefix of both is equal to directory
    # e.g. /a/b/c/d.rst and directory is /a/b, the common prefix is /a/b
    return os.path.commonprefix([file, directory]) == directory