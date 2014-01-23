import itertools as it
from inspect import getargspec, getcallargs
import os
import json
import re
from sqlalchemy import Column, Boolean, Integer, String, PickleType, ForeignKey, DateTime, func, Table, \
    UniqueConstraint, event
from sqlalchemy.orm import relationship, synonym, backref
from sqlalchemy.ext.declarative import declared_attr
from flask import url_for

from ..util.helpers import parse_cmd, kosmos_format, groupby
from .TaskFile import TaskFile
from ..db import Base
from ..util.sqla import Enum34_ColumnType
from .. import TaskStatus, StageStatus, signal_task_status_change


opj = os.path.join
class ToolValidationError(Exception): pass

class Tool(object):
    def __init__(self, *args, **kwargs):
        """
        :param tags: (dict) A dictionary of tags.
        :param stage: (str) The stage this task belongs to.
        """
        #if len(tags)==0: raise ToolValidationError('Empty tag dictionary.  All tasks should have at least one tag.')
        for attr in ['mem_req', 'time_req', 'cpu_req', 'must_succeed']:
            if hasattr(self.Defaults, attr):
                setattr(self, attr, getattr(self.Defaults, attr))

        self.settings = {}
        self.parameters = {}
        if not hasattr(self, 'inputs'): self.inputs = []
        if not hasattr(self, 'outputs'): self.outputs = []
        if not hasattr(self, 'forward_inputs'): self.forward_inputs = []

        self.tags = {k: str(v) for k, v in self.tags.items()}


        self._validate()

    def map_inputs(self, parents):
        """
        Default method to map inputs.  Can be overriden if a different behavior is desired
        :returns: (dict) A dictionary of taskfiles which are inputs to this task.  Keys are names of the taskfiles, values are a list of taskfiles.
        """
        if not self.inputs:
            return {}

        else:
            if '*' in self.inputs:
                return {'*': [o for p in parents for o in p.taskfiles]}

            all_inputs = filter(lambda x: x is not None,
                                [p.get_output(name, error_if_missing=False) for p in parents for name in
                                 self.inputs])

            input_dict = dict(
                (name, list(input_files)) for name, input_files in groupby(all_inputs, lambda i: i.name))

            for k in self.inputs:
                if k not in input_dict or len(input_dict[k]) == 0:
                    raise ValueError, "Could not find input '{0}' for {1}".format(k, self)

            return input_dict

    def generate_cmd(self):
        """
        Calls map_inputs() and processes the output of cmd()
        """
        p = self.parameters.copy()
        argspec = getargspec(self.cmd)

        for k, v in self.tags.items():
            if k in argspec.args:
                p[k] = v

        ## Validation
        # Helpful error message
        if not argspec.keywords: #keywords is the **kwargs name or None if not specified
            for k in p.keys():
                if k not in argspec.args:
                    raise ToolValidationError, '{0} received the parameter "{1}" which is not defined in it\'s signature.  Parameters are {2}.  Accept the parameter **kwargs in cmd() to generalize the parameters accepted.'.format(
                        self, k, argspec.args)

        for l in ['i', 'o', 's']:
            if l in p.keys():
                raise ToolValidationError, "{0} is a reserved name, and cannot be used as a tag keyword".format(l)

        try:
            kwargs = dict(i=self.map_inputs(), o={o.name: o for o in self.taskfiles}, s=self.settings, **p)
            callargs = getcallargs(self.cmd, **kwargs)
        except TypeError:
            raise TypeError, 'Invalid parameters for {0}.cmd(): {1}'.format(self, kwargs.keys())

        del callargs['self']
        r = self.cmd(**callargs)

        #if tuple is returned, second element is a dict to format with
        extra_format_dict = r[1] if len(r) == 2 and r else {}
        pcmd = r[0] if len(r) == 2 else r

        #format() return string with callargs
        callargs['self'] = self
        callargs.update(extra_format_dict)
        cmd = kosmos_format(pcmd, callargs)

        #fix TaskFiles paths
        cmd = re.sub('<TaskFile\[\d+?\] (.+?)>', lambda x: x.group(1), cmd)

        return parse_cmd(cmd)


    def cmd(self, i, o, s, **kwargs):
        """
        Constructs the preformatted command string.  The string will be .format()ed with the i,s,p dictionaries,
        and later, $OUT.outname  will be replaced with a TaskFile associated with the output name `outname`

        :param i: (dict who's values are lists) Input TaskFiles.
        :param o: (dict) Output TaskFiles.
        :param s: (dict) Settings.
        :param kwargs: (dict) Parameters.
        :returns: (str|tuple(str,dict)) A preformatted command string, or a tuple of the former and a dict with extra values to use for
            formatting
        """
        raise NotImplementedError("{0}.cmd is not implemented.".format(self.__class__.__name__))

    def configure(self, settings={}, parameters={}):
        """
        """
        self.parameters = parameters
        self.settings = settings
        return self

    def _validate(self):
        #validate inputs are strs
        if any([not isinstance(i, str) for i in self.inputs]):
            raise ToolValidationError, "{0} has elements in self.inputs that are not of type str".format(self)

        if len(self.inputs) != len(set(self.inputs)):
            raise ToolValidationError(
                'Duplicate names in task.inputs detected in {0}.  Perhaps try using [1.ext,2.ext,...]'.format(self))

        # output_names = [o.name for o in self.taskfiles]
        # if len(output_names) != len(set(output_names)):
        #     raise ToolValidationError(
        #         'Duplicate names in task.taskfiles detected in {0}.  Perhaps try using [1.ext,2.ext,...] when defining outputs'.format(
        #             self))