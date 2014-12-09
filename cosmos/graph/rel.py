import re
import itertools as it

from ..util.helpers import groupby
from .. import RelationshipType


def is_func(x):
    return hasattr(x, '__call__')


class Relationship(object):
    """Abstract Class for the various relationship strategies"""

    type = None

    def __str__(self):
        m = re.search("^(\w).+2(\w).+$", type(self).__name__)
        return '{0}2{1}'.format(m.group(1), m.group(2))


class RelationshipError(Exception): pass


class One2one(Relationship):
    type = RelationshipType.one2one

    @classmethod
    def gen_task_tags(klass, stage):
        for parent_task in it.chain(*(s.tasks for s in stage.parents)):
            tags2 = parent_task.tags.copy()
            tags2.update(stage.extra_tags)
            yield tags2, [parent_task]


class Many2one(Relationship):
    """
    Group parent tasks by reduce_by.  If a parent task is missing a keyword it acts as a wildcard. ex:
    {a:1, b:2, c:5}, {a:1, b:2, c:5}, {a:1, b:2} is a valid group using reduce_by ['a','b','c'] or ['a','b'].
    """
    type = RelationshipType.many2one

    def __init__(self, reduce_by=None):
        if reduce_by is None:
            reduce_by = []
        Many2one.validate_reduce_by(reduce_by)
        self.reduce_by = reduce_by

    @classmethod
    def validate_reduce_by(cls, reduce_by):
        """

        :param reduce_by: a list of strings or a function who's input is a  ist of parents, and who's output is (dict of tags, parents)
        :return:
        """
        assert isinstance(reduce_by, list) or is_func(reduce_by), '`reduce_by` must be a list or function'
        if isinstance(reduce_by, list):
            if any(k == '' for k in reduce_by):
                raise TypeError('keyword cannot be an empty string')

    @classmethod
    def gen_task_tags(cls, stage):
        for tags, parent_task_group in Many2one.reduce(stage, stage.rel.reduce_by):
            tags.update(stage.extra_tags)
            yield tags, parent_task_group

    @classmethod
    def default_reduce_by(cls, reduce_by, parent_tasks):
        def wildcard_subset(dict1, dict2):
            """
            :return: True if all items in dict1 are equivalent or missing in dict2.
            """
            return all(dict2.get(k, v) == v for k, v in dict1.items())

        def only_reduce_by(task):
            return dict((k, task.tags[k]) for k in reduce_by if k in task.tags)

        parent_tasks_without_all_reduce_by = filter(lambda t: not all(k in t.tags for k in reduce_by), parent_tasks)
        parent_tasks_with_all_reduce_by = filter(lambda t: all(k in t.tags for k in reduce_by), parent_tasks)

        if len(parent_tasks_with_all_reduce_by) == 0:
            raise RelationshipError('Parent stages must have at least one task with all many2one reduce_by of %s' % reduce_by)

        for tags, parent_task_group in groupby(parent_tasks_with_all_reduce_by, only_reduce_by):
            subsets = filter(lambda t: wildcard_subset(t.tags, tags), parent_tasks_without_all_reduce_by)
            parent_task_group = list(parent_task_group) + subsets
            yield tags, parent_task_group


    @classmethod
    def reduce(cls, stage, reduce_by):
        parent_tasks = list(it.chain(*(s.tasks for s in stage.parents)))
        if is_func(reduce_by):
            for tags, parent_task_group in reduce_by(parent_tasks=parent_tasks):
                from .. import Task

                assert isinstance(tags, dict), 'custom reduce_by function for %s did not yield a (dict, (list_of_parents)) tuple'
                assert all(it.imap(lambda t: issubclass(t.__class__, Task), parent_task_group)),\
                    '%s produced a parent_task_group that is not a list of Tasks: %s' % (reduce_by, parent_task_group)
                yield tags, parent_task_group
        else:
            for tags, parent_task_group in Many2one.default_reduce_by(reduce_by, parent_tasks):
                yield tags, parent_task_group


class One2many(Relationship):
    type = RelationshipType.one2many

    def __init__(self, split_by):
        One2many.validate_split_by(split_by)
        self.split_by = split_by

    @classmethod
    def validate_split_by(cls, split_by):
        assert isinstance(split_by, list) or is_func(split_by), '`split_by` must be a list or function'
        if isinstance(split_by, list) and len(split_by) > 0:
            assert isinstance(split_by[0], tuple), '`split_by` must be a list of tuples'
            assert isinstance(split_by[0][0], str), 'the first element of tuples in `split_by` must be a str'
            assert isinstance(split_by[0][1],
                              list), 'the second element of the tuples in the `split_by` list must also be a list'

    @classmethod
    def gen_task_tags(klass, stage):
        for parent_task in it.chain(*(s.tasks for s in stage.parents)):
            for tags in One2many.split(stage.rel.split_by, parent_task):
                tags.update(parent_task.tags)
                tags.update(stage.extra_tags)
                yield tags, [parent_task]

    @classmethod
    def default_split_by(cls, parent_task, split_by):
        splits = [list(it.product([split[0]], split[1])) for split in split_by]
        for new_tags in it.imap(dict, it.product(*splits)):
            yield new_tags


    @classmethod
    def split(cls, split_by, parent_task):
        # if is_func(split_by):
        # assert parent_task is not None, 'need a parent_task if split_by is a function'
        if is_func(split_by):
            for new_tags in split_by(parent_task=parent_task):
                assert isinstance(new_tags, dict), 'split_by function did not return a dict'
                yield new_tags
        else:
            for new_tags in One2many.default_split_by(parent_task, split_by):
                yield new_tags


class Many2many(Relationship):
    type = RelationshipType.many2many

    def __init__(self, reduce_by, split_by):
        assert isinstance(reduce_by, list), '`reduce_by` must be a list'
        self.reduce_by = reduce_by

        One2many.validate_split_by(split_by)
        self.split_by = split_by

    @classmethod
    def gen_task_tags(klass, stage):
        for tags, parent_task_group in Many2many.reduce_split(stage):
            tags.update(stage.extra_tags)
            # TODO only instantiate once
            yield tags, parent_task_group

    @classmethod
    def reduce_split(klass, stage):
        for tags, parent_task_group in Many2one.reduce(stage, stage.rel.reduce_by):
            for new_tags in One2many.split(stage.rel.split_by, None):
                new_tags.update(tags)
                yield new_tags, parent_task_group