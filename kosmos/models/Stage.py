from ..helpers import validate_name, validate_is_type_or_list
from . import rel as _rel
from .Task import Task
from ..db import Base
from sqlalchemy import Column, Integer, String, Boolean, DateTime, func

class Stage(Base):
    __tablename__ = 'stage'

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)

    def __init__(self, name, task_class=None, parents=None, rel=None, extra_tags=None, tasks=None, is_source=False):
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
        self.is_finished = False

        validate_name(self.name, 'name')


    @property
    def label(self):
        return '{0} (x{1})'.format(self.name, len(self.tasks))

    def __str__(self):
        return '<Stage {0}>'.format(self.name)

