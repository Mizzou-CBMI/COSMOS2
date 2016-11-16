import re
from sqlalchemy.schema import Column, ForeignKey, UniqueConstraint
from sqlalchemy.types import Boolean, Integer, String, DateTime
from sqlalchemy.orm import relationship, synonym, backref, validates
from sqlalchemy.ext.declarative import declared_attr
from flask import url_for

from ..db import Base
from ..util.sqla import Enum34_ColumnType
from .. import StageStatus, signal_stage_status_change, RelationshipType, TaskStatus
import networkx as nx
import datetime


@signal_stage_status_change.connect
def task_status_changed(stage):
    stage.log.info('%s %s (%s/%s successful tasks)' % (stage, stage.status, sum(t.successful for t in stage.tasks), len(stage.tasks)))

    if stage.status == StageStatus.successful:
        stage.successful = True

    if stage.status == StageStatus.running:
        stage.started_on = datetime.datetime.now()
    elif stage.status in [StageStatus.successful, StageStatus.failed, StageStatus.killed]:
        stage.finished_on = datetime.datetime.now()

    stage.session.commit()


class StageEdge(Base):
    __tablename__ = 'stage_edge'
    parent_id = Column(Integer, ForeignKey('stage.id', ondelete="CASCADE"), primary_key=True)
    child_id = Column(Integer, ForeignKey('stage.id', ondelete="CASCADE"), primary_key=True)

    def __init__(self, parent=None, child=None):
        self.parent = parent
        self.child = child

    def __str__(self):
        return '<StageEdge: %s -> %s>' % (self.parent, self.child)

    def __repr__(self):
        return self.__str__()


class Stage(Base):
    __tablename__ = 'stage'
    __table_args__ = (UniqueConstraint('workflow_id', 'name', name='_uc_workflow_name'),)

    id = Column(Integer, primary_key=True)
    number = Column(Integer)
    name = Column(String(255))
    started_on = Column(DateTime)
    workflow_id = Column(ForeignKey('workflow.id', ondelete="CASCADE"), nullable=False, index=True)
    started_on = Column(DateTime)
    finished_on = Column(DateTime)
    # relationship_type = Column(Enum34_ColumnType(RelationshipType))
    successful = Column(Boolean, nullable=False, default=False)
    _status = Column(Enum34_ColumnType(StageStatus), default=StageStatus.no_attempt)
    parents = relationship("Stage",
                           secondary=StageEdge.__table__,
                           primaryjoin=id == StageEdge.parent_id,
                           secondaryjoin=id == StageEdge.child_id,
                           backref="children",
                           passive_deletes=True,
                           cascade="save-update, merge, delete",
                           )
    tasks = relationship("Task", backref="stage", cascade="all, merge, delete-orphan", passive_deletes=True)

    @declared_attr
    def status(cls):
        def get_status(self):
            return self._status

        def set_status(self, value):
            if self._status != value:
                self._status = value
                signal_stage_status_change.send(self)

        return synonym('_status', descriptor=property(get_status, set_status))

    def __init__(self, *args, **kwargs):
        super(Stage, self).__init__(*args, **kwargs)

        if not re.match('^[a-zA-Z0-9_\.-]+$', self.name):
            raise Exception('invalid stage name %s' % self.name)

    def __iter__(self):
        for t in self.tasks:
            yield t

    def __getitem__(self, key):
        return self.tasks[key]

    @property
    def tasksq(self):
        from .Task import Task

        return self.session.query(Task)

    #
    # def num_tasks(self):
    #     return self.tasksq.count()

    def num_successful_tasks(self):
        # return self.tasksq.filter_by(stage=self, successful=True).count()
        return len(filter(lambda t: t.successful, self.tasks))

    def num_failed_tasks(self):
        # return self.tasksq.filter_by(stage=self, status=TaskStatus.failed).count()
        return len(filter(lambda t: t.status == TaskStatus.failed, self.tasks))

    @property
    def url(self):
        return url_for('cosmos.stage', workflow_name=self.workflow.name, stage_name=self.name)

    @property
    def log(self):
        return self.workflow.log

    def delete(self, delete_files=False, delete_descendants=False):
        """
        Deletes this stage
        :param delete_files: Delete all files (will be slow if there are a lot of files)
        :param delete_descendants: Also delete all delete_descendants of this stage
        :return: None
        """
        if delete_descendants:
            self.log.info('Deleting all delete_descendants of %s' % self)
            for stage in reversed(list(self.descendants())):
                stage.delete(delete_files)

        self.log.info('Deleting %s. delete_files=%s' % (self, delete_files))
        if delete_files:
            for t in self.tasks:
                t.delete(delete_files=True)
        self.session.delete(self)
        self.session.commit()

    def filter_tasks(self, **filter_by):
        return (t for t in self.tasks if all(t.params.get(k, None) == v for k, v in filter_by.items()))

    def get_task(self, uid, default='ERROR@#$'):
        # params = {k: v for k, v in params.items() if
        #         any(isinstance(v, t) for t in ACCEPTABLE_TAG_TYPES)}  # These are the only params that actually get saved to the DB
        for task in self.tasks:
            if task.uid == uid:
                return task

        if default == 'ERROR@#$':
            raise KeyError('Task with uid %s does not exist' % uid)
        else:
            return default

    # def get_task(self, **filter_by):
    #     tasks = self.filter_tasks(**filter_by)
    #     assert len(tasks) > 0, 'no task found with params %s' % filter_by
    #     assert len(tasks) == 1, 'more than one task with params %s' % filter_by
    #     return tasks[0]

    def percent_successful(self):
        return round(float(self.num_successful_tasks()) / (float(len(self.tasks)) or 1) * 100, 2)

    def percent_failed(self):
        return round(float(self.num_failed_tasks()) / (float(len(self.tasks)) or 1) * 100, 2)

    def percent_running(self):
        return round(float(len([t for t in self.tasks if t.status == TaskStatus.submitted])) / (
            float(len(self.tasks)) or 1) * 100, 2)

    def descendants(self, include_self=False):
        """
        :return: (list) all stages that descend from this stage in the stage_graph
        """
        # return set(it.chain(*breadth_first_search.bfs_successors(self.ex.stage_graph(), self).values()))
        x = nx.descendants(self.workflow.stage_graph(), self)
        if include_self:
            return sorted({self}.union(x), key=lambda stage: stage.number)
        else:
            return x

    @property
    def label(self):
        return '{0} ({1}/{2})'.format(self.name, self.num_successful_tasks(), len(self.tasks))

    def __repr__(self):
        return '<Stage[%s] %s>' % (self.id or '', self.name)
