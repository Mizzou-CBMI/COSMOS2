import shutil
import os
from sqlalchemy import Column, String, ForeignKey, Integer, UniqueConstraint, Table, Boolean
from sqlalchemy.orm import relationship, backref
from collections import namedtuple

from ..db import Base


class TaskFileValidationError(Exception): pass


class TaskFileError(Exception): pass


association_table = Table('input_files', Base.metadata,
                          Column('task', Integer, ForeignKey('task.id')),
                          Column('taskfile', Integer, ForeignKey('taskfile.id')))


class taskfile_dict(dict):
    _output_taskfile = False
    _input_taskfile = False

    @property
    def name(self):
        return self['name']

    @property
    def format(self):
        return self['format']

    @property
    def basename(self):
        if self._input_taskfile:
            return self['basename']
        else:
            raise AssertionError, 'taskfile_dict.output_taskfiles have no basename'

def output_taskfile(name=None, format=None, basename=None):
    assert name and format, 'must specify name and format'
    d = taskfile_dict(name=name, format=format, basename=basename)
    d._output_taskfile = True
    return d


def input_taskfile(name=None, format=None):
    assert name or format, 'must specify either name or format'
    d = taskfile_dict(name=name, format=format)
    d._input_taskfile = True
    return d


class TaskFile(Base):
    """
    Task File.
    """
    __tablename__ = 'taskfile'
    __table_args__ = (UniqueConstraint('task_output_for_id', 'name', 'format', name='_uc1'),)

    id = Column(Integer, primary_key=True)
    task_output_for_id = Column(ForeignKey('task.id'))
    task_output_for = relationship("Task", backref=backref('output_files', cascade="all, delete-orphan"))
    tasks_input_for = relationship("Task", backref=backref('input_files'), secondary=association_table)
    path = Column(String(255))
    name = Column(String(255), nullable=False)
    format = Column(String(255), nullable=False)
    #format = Column(String(255), nullable=False)
    basename = Column(String(255), nullable=False)
    persist = Column(Boolean)

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
            if not self.path.startswith(self.execution.output_dir):
                self.log.warn('Not deleting %s, outside of %s'%(self.path, self.execution.output_dir))
            else:
                if os.path.isdir(self.path):
                    shutil.rmtree(self.path)
                else:
                    os.remove(self.path)
        self.session.delete(self)
        self.session.commit()
