import shutil
import os
from ..db import Base
from sqlalchemy import Column, String, ForeignKey, Integer, UniqueConstraint, Table, Boolean
from sqlalchemy.orm import relationship, backref


class TaskFileValidationError(Exception): pass


class TaskFileError(Exception): pass


association_table = Table('input_files', Base.metadata,
                          Column('task', Integer, ForeignKey('task.id')),
                          Column('taskfile', Integer, ForeignKey('taskfile.id')))


class TaskFile(Base):
    """
    Task File
    """
    __tablename__ = 'taskfile'
    __table_args__ = (UniqueConstraint('task_output_for_id', 'name', name='_uc1'),)

    id = Column(Integer, primary_key=True)
    task_output_for_id = Column(ForeignKey('task.id'))
    task_output_for = relationship("Task", backref=backref('output_files', cascade="all, delete-orphan"))
    tasks_input_for = relationship("Task", backref=backref('input_files'), secondary=association_table)
    path = Column(String(255))
    name = Column(String(255), nullable=False)
    #format = Column(String(255), nullable=False)
    basename = Column(String(255))
    persist = Column(Boolean)

    @property
    def prefix(self):
        return self.basename.split('.')[0]

    def __init__(self, *args, **kwargs):
        super(TaskFile, self).__init__(*args, **kwargs)
        if self.basename is None:
            self.basename = '%s.%s' % (self.task_output_for.stage.name.lower(), self.name)

    def __repr__(self):
        return '<TaskFile[%s] %s:%s>' % (self.id or 'id_%s' % id(self), self.name, self.path or 'no_path_yet')
        #return '<TaskFile[%s] %s%s>' % (self.id or '', self.name, ' '+self.path or '')

    def delete(self, delete_file=True):
        """
        Deletes this task and all files associated with it
        """
        if delete_file and os.path.exists(self.path):
            if os.path.isdir(self.path):
                shutil.rmtree(self.path)
            else:
                os.remove(self.path)
        self.session.delete(self)
        self.session.commit()
