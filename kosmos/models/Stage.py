import re

from sqlalchemy import Column, Integer, String, DateTime, func, ForeignKey, UniqueConstraint, PickleType
from sqlalchemy.orm import relationship, synonym
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy import inspect, Table
from flask import url_for

from ..util.helpers import validate_is_type_or_list
from . import rel as _rel
from .Task import Task
from ..db import Base
from ..util.sqla import Enum34_ColumnType
from .. import StageStatus, TaskStatus, signal_stage_status_change


@signal_stage_status_change.connect
def task_status_changed(stage):
    print 'status %s %s' % (stage, stage.status)
    if stage.status == StageStatus.running:
        stage.started_on = func.now()
    elif stage.status == StageStatus.finished:
        stage.finished_on = func.now()

    inspect(stage).session.commit()

# stage_edge_table = Table('stage_edge', Base.metadata,
#                    Column('parent_id', Integer, ForeignKey('stage.id'), primary_key=True),
#                    Column('child_id', Integer, ForeignKey('stage.id'), primary_key=True)
# )

class Stage(Base):
    __tablename__ = 'stage'
    __table_args__ = (UniqueConstraint('execution_id', 'name', name='_uc1'),)

    id = Column(Integer, primary_key=True)
    name = Column(String)
    started_on = Column(DateTime)
    finished_on = Column(DateTime)
    execution_id = Column(ForeignKey('execution.id'))
    execution = relationship("Execution", backref="stages")
    started_on = Column(DateTime)
    finished_on = Column(DateTime)
    stage_graph = Column(PickleType)
    # parents = relationship("Task",
    #                        secondary=stage_edge_table,
    #                        primaryjoin=id == stage_edge_table.c.parent_id,
    #                        secondaryjoin=id == stage_edge_table.c.child_id,
    #                        backref="children"
    # )
    _status = _status = Column(Enum34_ColumnType(StageStatus), default=StageStatus.no_attempt)


    @declared_attr
    def status(cls):
        def get_status(self):
            return self._status

        def set_status(self, value):
            self._status = value
            signal_stage_status_change.send(self)
        return synonym('_status', descriptor=property(get_status, set_status))


    def __init__(self, name, task_class=None, parents=None, rel=None, extra_tags=None, tasks=None, is_source=False):
        if name is None:
            if hasattr(task_class, 'name'):
                name = task_class.name
            else:
                name = task_class.__name__

        assert name is not None, 'name cannot be None'
        if parents is None:
            parents = []
        if tasks is None:
            tasks = []
        if tasks and task_class and not is_source:
            raise TypeError, 'cannot initialize with both a `task` and `tasks` unless `is_source`=True'
        if extra_tags is None:
            extra_tags = {}
        if rel == _rel.One2one or rel is None:
            rel = _rel.One2one()

        assert issubclass(task_class, Task), '`task` must be a subclass of `Task`'
        # assert rel is None or isinstance(rel, Relationship), '`rel` must be of type `Relationship`'

        self.task_class = task_class
        self.tasks = tasks
        self.parents = validate_is_type_or_list(parents, Stage)
        self.rel = rel
        self.is_source = is_source
        self.resolved = False

        self.extra_tags = extra_tags
        self.name = name

        if not re.match('^[a-zA-Z0-9_\.-]+$',self.name):
            raise Exception, 'invalid stage name %s' % self.name


    @property
    def url(self):
        return url_for('.stage', id=self.id)

    @property
    def label(self):
        return '{0} (x{1})'.format(self.name, len(self.tasks))

    def __repr__(self):
        return '<Stage[%s] %s>' % (self.id or '',self.name)

