import shutil
import os

from sqlalchemy import Column, String, ForeignKey, Integer, UniqueConstraint, Table, Boolean
from sqlalchemy.orm import relationship, backref

from recordtype import recordtype
from ..db import Base


class TaskFileValidationError(Exception): pass


class TaskFileError(Exception): pass


association_table = Table('input_files', Base.metadata,
                          Column('task', Integer, ForeignKey('task.id')),
                          Column('taskfile', Integer, ForeignKey('taskfile.id')))

AbstractInputFile = recordtype('AbstractInputFile', ['name', 'format','forward'])
AbstractOutputFile = recordtype('AbstractOutputFile', ['name', 'format','basename'])


def output_taskfile(name=None, format=None, basename=None):
    assert name and format, 'must specify name and format'
    return AbstractOutputFile(name=name, format=format, basename=basename)


def input_taskfile(name=None, format=None, forward=False):
    assert name or format, 'must specify either name or format'
    return AbstractInputFile(name=name, format=format, forward=forward)



class InputFileAssociation(Base):
    __tablename__ = 'input_file_assoc'
    task_id = Column(Integer, ForeignKey('task.id'), primary_key=True)
    taskfile_id = Column(Integer, ForeignKey('taskfile.id'), primary_key=True)
    forward = Column(Boolean, default=False)
    taskfile = relationship("TaskFile", backref=backref("_input_file_assocs", cascade="all, delete-orphan", single_parent=True))
    task = relationship("Task", backref=backref("_input_file_assocs", cascade="all, delete-orphan", single_parent=True))

    def delete(self):
        self.task._input_file_assocs.remove(self)
        self.taskfile._input_file_assocs.remove(self)


class TaskFile(Base):
    """
    Task File.
    """
    __tablename__ = 'taskfile'
    __table_args__ = (UniqueConstraint('task_output_for_id', 'name', 'format', name='_uc_tf_name_fmt'),)

    id = Column(Integer, primary_key=True)
    task_output_for_id = Column(ForeignKey('task.id'))
    task_output_for = relationship("Task", backref=backref('output_files', cascade="all, delete-orphan"))
    path = Column(String(255))
    name = Column(String(255), nullable=False)
    format = Column(String(255), nullable=False)
    basename = Column(String(255), nullable=False)
    persist = Column(Boolean, default=False)

    @property
    def tasks_input_for(self):
        return [ ifa.task for ifa in self._input_file_assocs ]

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

    def __repr__(self):
        return '<TaskFile[%s] %s.%s:%s>' % (self.id or 'id_%s' % id(self), self.name, self.format, self.path or 'no_path_yet')

    def delete(self, delete_file=True):
        """
        Deletes this task and all files associated with it
        """
        self.log.debug('Deleting %s' % self)

        if not self.task_output_for.NOOP and delete_file and os.path.exists(self.path):
            if not in_directory(self.path, self.execution.output_dir):
                self.log.warn('Not deleting %s, outside of %s' % (self.path, self.execution.output_dir))
            else:
                if os.path.isdir(self.path):
                    shutil.rmtree(self.path)
                else:
                    os.remove(self.path)

        self.session.delete(self)
        #self.session.commit()


def in_directory(file, directory):
    # make both absolute
    directory = os.path.join(os.path.realpath(directory), '')
    file = os.path.realpath(file)

    # return true, if the common prefix of both is equal to directory
    #e.g. /a/b/c/d.rst and directory is /a/b, the common prefix is /a/b
    return os.path.commonprefix([file, directory]) == directory