import itertools as it
import copy
from inspect import getargspec, getcallargs
import os

#cosmos_format
from .TaskFile import TaskFile
from .helpers import parse_cmd
from .helpers import cosmos_format


i = 0


def get_id():
    global i
    i += 1
    return i


files = []


class ExpectedError(Exception): pass


class ToolError(Exception): pass


class ToolValidationError(Exception): pass


class GetOutputError(Exception): pass


class Tool(object):
    """
    A Tool is a class who's instances represent a command that gets executed.  It also contains properties which
    define the resources that are required.

    :property stage: (str) The Tool's Stage
    :property dag: (ToolGraph) The dag that is keeping track of this Tool
    :property id: (int) A unique identifier.  Useful for debugging.
    :property input_files: (list) This Tool's input TaskFiles
    :property output_files: (list) This Tool's output TaskFiles.  A tool's output taskfile names should be unique.
    :property tags: (dict) This Tool's tags.
    """
    #TODO props that cant be overridden should be private

    #: (list of strs) a list of input names.
    inputs = None
    #: (list of strs or TaskFiles) a list of output names. Default is [].
    outputs = None
    #: (int) Number of megabytes of memory to request.  Default is 1024.
    mem_req = 1 * 1024
    #: (int) Number of cores to request.  Default is 1.
    cpu_req = 1
    #: (int) Number of minutes to request. Default is 1.
    time_req = None #(mins)
    #: (bool) If True, these tasks do not contain commands that are executed.  Used for INPUT.  Default is False.
    NOOP = False
    #: (bool) If True, if this tool's tasks' job attempts fail, the task will still be considered successful.  Default is False.
    succeed_on_failure = False
    #: (dict) A dictionary of default parameters.  Default is {}.
    default_params = None
    #: (bool) If True, output_files described as a str in outputs will be by default be created with persist=True.
    #: If delete_interemediates is on, they will not be deleted.
    persist = False
    #: forwards this tool's input to get_output() calls
    # forward_input = False
    #: always run job as a subprocess even when DRM is not set to local
    always_local = False

    def __init__(self, tags, stage=None, dag=None):
        """
        :param stage: (str) The stage this tool belongs to. Required.
        :param tags: (dict) A dictionary of tags.
        :param dag: The dag this task belongs to.
        :param parents: A list of tool instances which this tool is dependent on
        """
        #if len(tags)==0: raise ToolValidationError('Empty tag dictionary.  All tools should have at least one tag.')
        if not hasattr(self, 'name'): self.name = self.__class__.__name__
        if not hasattr(self, 'output_files'): self.output_files = []
        if not hasattr(self, 'settings'): self.settings = {}
        if not hasattr(self, 'parameters'): self.parameters = {}
        if self.inputs is None: self.inputs = []
        if self.outputs is None: self.outputs = []
        if self.default_params is None: self.default_params = {}

        self.stage = stage
        for k, v in tags.copy().items():
            tags[k] = str(v)
        self.tags = tags
        self.dag = dag

        # Because defining attributes in python creates a reference to a single instance across all class instance
        # any taskfile instances in self.outputs is used as a template for instantiating a new class
        self.outputs = [copy.copy(o) if isinstance(o, TaskFile) else o for o in self.outputs]
        self.id = get_id()

        # Create empty output TaskFiles
        for output in self.outputs:
            if isinstance(output, TaskFile):
                self.add_output(output)
            elif isinstance(output, str):
                tf = TaskFile(name=output)
                self.add_output(tf)
            else:
                raise ToolValidationError, "{0}.outputs must be a list of strs or Taskfile instances.".format(self)

        #validate inputs are strs
        if any([not isinstance(i, str) for i in self.inputs]):
            raise ToolValidationError, "{0} has elements in self.inputs that are not of type str".format(self)

        if len(self.inputs) != len(set(self.inputs)):
            raise ToolValidationError(
                'Duplicate names in tool.inputs detected in {0}.  Perhaps try using [1.ext,2.ext,...]'.format(self))

        output_names = [o.name for o in self.output_files]
        if len(output_names) != len(set(output_names)):
            raise ToolValidationError(
                'Duplicate names in tool.output_files detected in {0}.  Perhaps try using [1.ext,2.ext,...] when defining outputs'.format(
                    self))

    @property
    def children(self):
        return self.dag.tool_G.successors(self)

    @property
    def parents(self):
        return self.dag.tool_G.predecessors(self)

    @property
    def parent(self):
        ps = self.parents
        if len(ps) > 1:
            raise ToolError('{0} has more than one parent.  The parents are: {1}'.format(self, self.parents))
        elif len(ps) == 0:
            raise ToolError('{0} has no parents'.format(self))
        else:
            return ps[0]

    def get_output(self, name, error_if_missing=True):
        for o in self.output_files:
            if o.name == name:
                return o

        if error_if_missing:
            raise ToolError, 'Output named `{0}` does not exist in {1}'.format(name, self)

    def add_output(self, taskfile):
        """
        Adds an taskfile to self.output_files
        
        :param taskfile: an instance of a TaskFile
        """
        self.output_files.append(taskfile)

    @property
    def input_files(self):
        "A list of input TaskFiles"
        return list(it.chain(*[tf for tf in self.map_inputs().values()]))

    @property
    def label(self):
        "Label used for the ToolGraph image"
        tags = '' if len(self.tags) == 0 else "\\n {0}".format(
            "\\n".join(["{0}: {1}".format(k, v) for k, v in self.tags.items()]))
        return "[{3}] {0}{1}".format(self.name, tags, self.pcmd, self.id)

    def map_inputs(self):
        """
        Default method to map inputs.  Can be overriden if a different behavior is desired
        :returns: (dict) A dictionary of taskfiles which are inputs to this tool.  Keys are names of the taskfiles, values are a list of taskfiles.
        """
        if not self.inputs:
            return {}

        else:
            if '*' in self.inputs:
                return {'*': [o for p in self.parents for o in p.output_files]}

            all_inputs = filter(lambda x: x is not None,
                                [p.get_output(name, error_if_missing=False) for p in self.parents for name in
                                 self.inputs])

            input_dict = dict(
                (name, list(input_files)) for name, input_files in it.groupby(all_inputs, lambda i: i.name))

            for k in self.inputs:
                if k not in input_dict or len(input_dict[k]) == 0:
                    raise ToolValidationError, "Could not find input '{0}' for {1}".format(k, self)

            return input_dict


    @property
    def pcmd(self):
        return self.process_cmd() if not self.NOOP else ''

    def process_cmd(self):
        """
        Calls map_inputs() and processes the output of cmd()
        """
        p = self.parameters.copy()
        argspec = getargspec(self.cmd)

        for k, v in self.tags.items():
            if k in argspec.args:
                p[k] = v

        # Helpful error message
        if not argspec.keywords: #keywords is the **kwargs name or None if not specified
            for k in p.keys():
                if k not in argspec.args:
                    raise ToolValidationError, '{0} received the parameter "{1}" which is not defined in it\'s signature.  Parameters are {2}.  Accept the parameter **kwargs in cmd() to generalize the parameters accepted.'.format(
                        self, k, tool_parameter_names)

        for l in ['i', 'o', 's']:
            if l in p.keys():
                raise ToolValidationError, "{0} is a reserved name, and cannot be used as a tag keyword".format(l)

        try:
            kwargs = dict(i=self.map_inputs(), o={o.name: o for o in self.output_files}, s=self.settings, **p)
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
        return parse_cmd(cosmos_format(*self.post_cmd(pcmd, callargs)))

    def post_cmd(self, cmd_str, format_dict):
        """
        Provides an opportunity to make any last minute changes to the cmd generated.

        :param cmd_str: (str) the string returned by cmd
        :param format_dict: (str) the dictionary that cmd was about to be .format()ed with
        :returns: (str,dict) the post_processed cmd_str and format_dict
        """
        return cmd_str, format_dict


    def cmd(self, i, s, p):
        """
        Constructs the preformatted command string.  The string will be .format()ed with the i,s,p dictionaries,
        and later, $OUT.outname  will be replaced with a TaskFile associated with the output name `outname`

        :param i: (dict) Input TaskFiles.
        :param s: (dict) Settings.  The settings dictionary, set by using :py:meth:`lib.ezflow.dag.configure`
        :param p: (dict) Parameters.
        :returns: (str|tuple(str,dict)) A preformatted command string, or a tuple of the former and a dict with extra values to use for
            formatting
        """
        raise NotImplementedError("{0}.cmd is not implemented.".format(self.__class__.__name__))

    def __str__(self):
        return '<{0} {1}>'.format(self.__class__.__name__, self.tags)


class INPUT(Tool):
    """
    An Input File.

    Does not actually execute anything, but provides a way to load an input file.

    >>> INPUT('/path/to/file.ext',tags={'key':'val'})
    >>> INPUT(path='/path/to/file.ext.gz',name='ext',fmt='ext.gz',tags={'key':'val'})
    """
    name = "Load_Input_Files"
    NOOP = True
    mem_req = 0
    cpu_req = 0
    persist = True

    def __init__(self, path, tags, name=None, fmt=None, *args, **kwargs):
        """
        :param path: the path to the input file
        :param name: the name or keyword for the input file
        :param fmt: the format of the input file
        """
        path = os.path.abspath(path)
        super(INPUT, self).__init__(tags=tags, *args, **kwargs)
        self.add_output(TaskFile(path=path, name=name, fmt=fmt, persist=True))

    def __str__(self):
        return '[{0}] {1} {2}'.format(self.id, self.__class__.__name__, self.tags)