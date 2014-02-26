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
        for parent_task in it.chain(*[s.tasks for s in stage.parents]):
            tags2 = parent_task.tags.copy()
            tags2.update(stage.extra_tags)
            yield tags2, [parent_task]


class Many2one(Relationship):
    """
    Group parent tasks by keywords.  If a parent task is missing a keyword it acts as a wildcard. ex:
    {a:1, b:2, c:5}, {a:1, b:2, c:5}, {a:1, b:2} is a valid group using keywords ['a','b','c'] or ['a','b'].
    """
    type = RelationshipType.many2one

    def __init__(self, keywords=None):
        if keywords is None:
            keywords = []
        assert isinstance(keywords, list), '`keywords` must be a list'
        self.keywords = keywords

    @classmethod
    def gen_task_tags(klass, stage):
        for tags, parent_task_group in Many2one.reduce(stage, stage.rel.keywords):
            tags.update(stage.extra_tags)
            yield tags, parent_task_group

    @classmethod
    def reduce(cls, stage, keywords):
        if type(keywords) != list:
            raise TypeError('keywords must be a list')
        if any(k == '' for k in keywords):
            raise TypeError('keyword cannot be an empty string')

        def wildcard_subset(dict1, dict2):
            """
            :return: True if all items in dict1 are equivalent or missing in dict2.
            """
            return all(dict2.get(k, v) == v for k, v in dict1.items())

        def only_keywords(task):
            return dict((k, task.tags[k]) for k in keywords if k in task.tags)

        parent_tasks = list(it.chain(*[s.tasks for s in stage.parents]))
        parent_tasks_without_all_keywords = filter(lambda t: not all([k in t.tags for k in keywords]),
                                                   parent_tasks)
        parent_tasks_with_all_keywords = filter(lambda t: all(k in t.tags for k in keywords), parent_tasks)

        if len(parent_tasks_with_all_keywords) == 0:
            raise RelationshipError(
                'Parent stages must have at least one task with all many2one keywords of %s' % keywords)

        for tags, parent_task_group in groupby(parent_tasks_with_all_keywords, only_keywords):
            subsets = filter(lambda t: wildcard_subset(t.tags, tags), parent_tasks_without_all_keywords)
            # if stage.name == 'CGACallDiff':
            #     import IPython;
            #
            #     IPython.embed()
            parent_task_group = list(parent_task_group) + subsets
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
        for parent_task in it.chain(*[s.tasks for s in stage.parents]):
            for tags in One2many.split(stage.rel.split_by, parent_task):
                tags.update(parent_task.tags)
                tags.update(stage.extra_tags)
                yield tags, [parent_task]

    @classmethod
    def split(cls, split_by, parent_task):
        def default_split_by(_):
            splits = [list(it.product([split[0]], split[1])) for split in split_by]
            return it.imap(dict, it.product(*splits))

        f = split_by if is_func(split_by) else default_split_by

        for new_tags in f(parent_task):
            assert isinstance(new_tags, dict), 'split_by function did not return a dict'
            yield new_tags


class Many2many(Relationship):
    type = RelationshipType.many2many

    def __init__(self, keywords, split_by):
        One2many.validate_split_by(split_by)
        self.split_by = split_by
        assert isinstance(keywords, list), '`keywords` must be a list'
        self.keywords = keywords

    @classmethod
    def gen_task_tags(klass, stage):
        for tags, parent_task_group in Many2many.reduce_split(stage):
            tags.update(stage.extra_tags)
            #TODO only instantiate once
            yield tags, parent_task_group

    @classmethod
    def reduce_split(klass, stage):
        for tags, parent_task_group in Many2one.reduce(stage, stage.rel.keywords):
            for new_tags in One2many.split(stage.rel.split_by):
                new_tags.update(tags)
                yield new_tags, parent_task_group