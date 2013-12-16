from .ToolGraph import one2one

class Stage():
    def __init__(self, name, task=None, parents=None, rel=None, extra_tags=None, tasks=None, is_source=False):
        if parents is None:
            parents = []
        if tasks is None:
            tasks = []
        if tasks and task and not is_source:
            raise TypeError, 'cannot initialize with both a `task` and `tasks` unless `is_source`=True'
        if extra_tags is None:
            extra_tags = {}
        if rel == one2one or rel is None:
            rel = one2one()

        assert issubclass(task, Task), '`task` must be a subclass of `Tool`'
        # assert rel is None or isinstance(rel, Relationship), '`rel` must be of type `Relationship`'

        self.task = task
        self.tasks = tasks
        self.parents = validate_is_type_or_list(parents, Stage)
        self.rel = rel
        self.is_source = is_source

        self.extra_tags = extra_tags
        self.name = name
        self.is_finished = False

        validate_name(self.name, 'name')


    @property
    def label(self):
        return '{0} (x{1})'.format(self.name, len(self.tasks))

    def __str__(self):
        return '<Stage {0}>'.format(self.name)

