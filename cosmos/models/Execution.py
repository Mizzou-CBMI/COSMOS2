import os
import re
import shutil
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.schema import Column
from sqlalchemy.types import Boolean, Integer, String, DateTime, VARCHAR
from sqlalchemy import orm
from sqlalchemy.sql.expression import func
from sqlalchemy.orm import validates, synonym, relationship, backref
from flask import url_for
import networkx as nx
from networkx.algorithms.dag import descendants, topological_sort
import atexit
from ..util.iterstuff import only_one
import sys
import types
from ..util.helpers import duplicates, groupby2
from ..db import Base
import time
import itertools as it
import datetime
from ..core.cmd_fxn import signature
from ..core.cmd_fxn import io
import funcsigs

opj = os.path.join
import signal

from .. import TaskStatus, StageStatus, ExecutionStatus, signal_execution_status_change
from .Task import Task


from ..util.helpers import get_logger
from ..util.sqla import Enum34_ColumnType, MutableDict, JSONEncodedDict, get_or_create


def _default_task_log_output_dir(task):
    """The default function for computing Task.log_output_dir"""
    return opj(task.execution.output_dir, 'log', task.stage.name, str(task.id))


def get_or_create_task(tool, successful_tasks, tags, stage, parents, default_drm):
    existing_task = successful_tasks.get(frozenset(tags.items()), None)
    if existing_task:
        existing_task.tool = tool
        return existing_task
    else:
        return tool._generate_task(stage=stage, parents=parents, default_drm=default_drm)


@signal_execution_status_change.connect
def _execution_status_changed(ex):
    if ex.status in [ExecutionStatus.successful, ExecutionStatus.failed, ExecutionStatus.killed]:
        logfunc = ex.log.warning if ex.status in [ExecutionStatus.failed, ExecutionStatus.killed] else ex.log.info
        logfunc('%s %s, output_dir: %s' % (ex, ex.status, ex.output_dir))
        ex.finished_on = datetime.datetime.now()

    if ex.status == ExecutionStatus.successful:
        ex.successful = True
        ex.finished_on = datetime.datetime.now()

    ex.session.commit()


class Execution(Base):
    """
    The primary object.  An Execution is an instantiation of a recipe being run.
    """
    __tablename__ = 'execution'

    id = Column(Integer, primary_key=True)
    name = Column(VARCHAR(200), unique=True)
    description = Column(String(255))
    successful = Column(Boolean, nullable=False, default=False)
    output_dir = Column(String(255), nullable=False)
    created_on = Column(DateTime)
    started_on = Column(DateTime)
    finished_on = Column(DateTime)
    max_cpus = Column(Integer)
    max_attempts = Column(Integer, default=1)
    info = Column(MutableDict.as_mutable(JSONEncodedDict))
    # recipe_graph = Column(PickleType)
    _status = Column(Enum34_ColumnType(ExecutionStatus), default=ExecutionStatus.no_attempt)
    stages = relationship("Stage", cascade="all, merge, delete-orphan", order_by="Stage.number", passive_deletes=True,
                          backref='execution')

    exclude_from_dict = ['info']

    @declared_attr
    def status(cls):
        def get_status(self):
            return self._status

        def set_status(self, value):
            if self._status != value:
                self._status = value
                signal_execution_status_change.send(self)

        return synonym('_status', descriptor=property(get_status, set_status))

    @validates('name')
    def validate_name(self, key, name):
        assert re.match(r"^[\w-]+$", name), 'Invalid execution name, characters are limited to letters, numbers, ' \
                                            'hyphens and underscores'
        return name

    @orm.reconstructor
    def constructor(self):
        self.__init__(manual_instantiation=False)

    def __init__(self, manual_instantiation=True, *args, **kwargs):
        if manual_instantiation:
            raise TypeError, 'Do not instantiate an Execution manually.  Use the Cosmos.start method.'
        super(Execution, self).__init__(*args, **kwargs)
        assert self.output_dir is not None, 'output_dir cannot be None'
        if self.info is None:
            # mutable dict column defaults to None
            self.info = dict()
        self.jobmanager = None
        if not self.created_on:
            self.created_on = datetime.datetime.now()
        self._task_references_to_stop_garbage_collection_which_destroys_tool_attribute = []

    def __getattr__(self, item):
        if item == 'log':
            self.log = get_logger('cosmos-%s' % Execution.name, opj(self.output_dir, 'execution.log'))
            return self.log
        else:
            raise AttributeError('%s is not an attribute of %s' % (item, self))

    def add_task(self, cmd_fxn, tags=None, parents=None, out_dir='', stage_name=None):
        """
        Adds a Task

        :param func cmd_fxn: A function that returns a str or NOOP.  It will be called when this Node is executed in the DAG.
        :param dict tags: A dictionary of key/value pairs to identify this Task, and to be passed as parameters to `cmd_fxn`
        :param list[Task] parents: List of dependencies
        :param str out_dir: Output directory (can be absolute or relative to execution output_dir)
        :param str stage_name: Name of the stage to add this task to
        :return: a Task
        """
        from .Stage import Stage

        if tags is None:
            tags = dict()
        if isinstance(parents, types.GeneratorType):
            parents = list(parents)
        if parents is None:
            parents = []
        if isinstance(parents, Task):
            parents = [parents]

        if out_dir:
            out_dir = out_dir.format(**tags)

        # for k, v in tags.items():
        #     if isinstance(v, basestring):
        #         tags[k] = v.format(**tags)

        # Get the right Stage
        if stage_name is None:
            stage_name = str(cmd_fxn.__name__).replace('_', ' ').title().replace(' ', '_')
        stage = only_one((s for s in self.stages if s.name == stage_name), None)
        if stage is None:
            stage = Stage(execution=self, name=stage_name)
            self.session.add(stage)

        # Check if task is already in stage
        task = stage.get_task(tags, None)

        if task is not None:
            # if task is already in stage, but unsuccessful, raise an error (duplicate tags) since unsuccessful tasks
            # were already removed on execution load
            if task.successful:
                return task
            else:
                # TODO check for duplicate tags here?  would be a lot faster at Execution.run
                raise ValueError('Duplicate tags, you have added a job to %s with tags %s twice' % (stage_name, tags))
        else:
            # Create Task
            attrs = {k: tags[k] for k in ['drm', 'mem_req', 'time_req', 'cpu_req', 'must_succeed'] if k in tags}
            if 'drm' not in attrs:
                attrs['drm'] = self.cosmos_app.default_drm

            input_map, output_map = io.get_io_map(cmd_fxn, tags, parents, stage.name, out_dir, self.output_dir)
            input_files = io.unpack_io_map(input_map)
            output_files = io.unpack_io_map(output_map)

            task = Task(stage=stage, tags=tags, parents=parents, input_files=input_files,
                        output_files=output_files, output_dir=out_dir,
                        **attrs)

            task.cmd_fxn = cmd_fxn
            task.input_map = input_map
            task.output_map = output_map

        # Add Stage Dependencies
        for p in parents:
            if p.stage not in stage.parents:
                stage.parents.append(p.stage)

        self._task_references_to_stop_garbage_collection_which_destroys_tool_attribute.append(task)

        return task



    def run(self, log_out_dir_func=_default_task_log_output_dir, dry=False, set_successful=True,
            cmd_wrapper=signature.default_cmd_fxn_wrapper):
        """
        Runs this Execution's DAG

        :param log_out_dir_func: (function) a function that computes a task's log_out_dir_func.
             It receives one parameter: the task instance.
             By default task log output is stored in output_dir/log/stage_name/task_id.
             See _default_task_log_output_dir for more info.
        :param dry: (bool) if True, do not actually run any jobs.
        :param set_successful: (bool) sets this execution as successful if all tasks finish without a failure.  You might set this to False if you intend to add and
            run more tasks in this execution later.
        """
        assert os.path.exists(os.getcwd()), 'current working dir does not exist! %s' % os.getcwd()

        assert hasattr(self, 'cosmos_app'), 'Execution was not initialized using the Execution.start method'
        assert hasattr(log_out_dir_func, '__call__'), 'log_out_dir_func must be a function'
        assert self.session, 'Execution must be part of a sqlalchemy session'
        session = self.session
        self.log.info('Preparing to run %s using DRM `%s`, output_dir: `%s`' % (
            self, self.cosmos_app.default_drm, self.output_dir))

        from ..job.JobManager import JobManager

        self.jobmanager = JobManager(cosmos_app=self.cosmos_app, get_submit_args=self.cosmos_app.get_submit_args,
                                     default_queue=self.cosmos_app.default_queue, cmd_wrapper=cmd_wrapper
                                     )

        self.status = ExecutionStatus.running
        self.successful = False

        if self.started_on is None:
            import datetime

            self.started_on = datetime.datetime.now()

        # Render task graph and to session
        # import ipdb
        # with ipdb.launch_ipdb_on_exception():
        #     print self.tasks
        task_g = self.task_graph()
        stage_g = self.stage_graph()

        # Set output_dirs of new tasks
        # for task in nx.topological_sort(task_g):
        # if not task.successful:
        # task.output_dir = task_output_dir(task)
        #         assert task.output_dir not in ['', None], "Computed an output file root_path of None or '' for %s" % task
        #         for tf in task.output_files:
        #             if tf.path is None:
        #                 tf.path = opj(task.output_dir, tf.basename)
        #                 assert tf.path is not None, 'computed an output_dir for %s of None' % task
        #                 # recipe_stage2stageprint task, tf.root_path, 'basename:',tf.basename

        # set commands of new tasks
        # for task in topological_sort(task_g):
        #     if not task.successful: # and not task.NOOP:
        #         task.command = task.tool._generate_command(task)

        import itertools as it

        # def assert_no_duplicate_taskfiles():
        #     taskfiles = (tf for task in task_g.nodes() for tf in task.output_files if not tf.duplicate_ok)
        #     f = lambda tf: tf.path
        #     for path, group in it.groupby(sorted(filter(lambda tf: not tf.task_output_for.NOOP, taskfiles), key=f), f):
        #         group = list(group)
        #         if len(group) > 1:
        #             t1 = group[0].task_output_for
        #             tf1 = group[0]
        #             t2 = group[1].task_output_for
        #             tf2 = group[1]
        #             div = "-" * 72 + "\n"
        #             self.log.error("Duplicate taskfiles paths detected:\n "
        #                            "{div}"
        #                            "{t1}\n"
        #                            "* {tf1}\n"
        #                            "{div}"
        #                            "{t2}\n"
        #                            "* {tf2}\n"
        #                            "{div}".format(**locals()))
        #
        #             raise ValueError('Duplicate taskfile paths')
        #
        # assert_no_duplicate_taskfiles()


        # Collapse
        # from ..graph.collapse import collapse
        #
        # for stage_bubble, name in recipe.collapses:
        #     self.log.debug('Collapsing %s into `%s`' % ([s.name for s in stage_bubble], name))
        #     collapse(session, task_g, stage_g, stage_bubble, name)

        # taskg and stageg are now finalized

        # stages = stage_g.nodes()
        assert len(set(self.stages)) == len(self.stages), 'duplicate stage name detected: %s' % (
            next(duplicates(self.stages)))

        # renumber stages
        for i, s in enumerate(topological_sort(stage_g)):
            s.number = i + 1

        # Add final taskgraph to session
        # session.expunge_all()
        session.add(self)
        # session.add_all(stage_g.nodes())
        # session.add_all(task_g.nodes())
        successful = filter(lambda t: t.successful, task_g.nodes())

        # commit so task.id is set for log dir
        self.log.info('Committing %s Tasks to the SQL database...' % (len(task_g.nodes()) - len(successful)))
        session.commit()

        # print stages
        for s in topological_sort(stage_g):
            self.log.info('%s %s' % (s, s.status))

        # Create Task Queue
        task_queue = _copy_graph(task_g)
        self.log.info('Skipping %s successful tasks...' % len(successful))
        task_queue.remove_nodes_from(successful)

        handle_exits(self)

        self.log.info('Setting log output directories...')

        def set_log_dirs():
            log_dirs = {t.log_dir: t for t in successful}
            for task in task_queue.nodes():
                log_dir = log_out_dir_func(task)
                assert log_dir not in log_dirs, 'Duplicate log_dir detected for %s and %s' % (task, log_dirs[log_dir])
                log_dirs[log_dir] = task
                task.log_dir = log_dir

        set_log_dirs()

        self.log.info('Checking stage attributes...')

        def reset_stage_attrs():
            """Update stage attributes if new tasks were added to them"""
            from .Stage import Stage
            from .. import StageStatus
            # using .update() threw an error, so have to do it the slow way. It's not too bad though, since
            # there shouldn't be that many stages to update.
            for s in session.query(Stage).join(Task).filter(~Task.successful, Stage.execution_id == self.id,
                                                            Stage.status != StageStatus.no_attempt):
                s.successful = False
                s.finished_on = None
                s.status = StageStatus.running

        reset_stage_attrs()


        if self.max_cpus is not None:
            self.log.info('Ensuring there are enough cores...')
            # make sure we've got enough cores
            for t in task_queue:
                assert t.cpu_req <= self.max_cpus, '%s requires more cpus (%s) than `max_cpus` (%s)' % (
                    t, t.cpu_req, self.max_cpus)

        # Run this thing!
        session.commit()
        if not dry:
            _run(self, session, task_queue)

            # set status
            if self.status == ExecutionStatus.failed_but_running:
                self.status = ExecutionStatus.failed
                # set stage status to failed
                for s in self.stages:
                    if s.status == StageStatus.running_but_failed:
                        s.status = StageStatus.failed
                session.commit()
                return False
            elif self.status == ExecutionStatus.running:
                if set_successful:
                    self.status = ExecutionStatus.successful
                session.commit()
                return True
            else:
                raise AssertionError('Bad execution status %s' % self.status)

        self.log.info('Execution complete')

    def terminate(self, due_to_failure=True):
        self.log.warning('Terminating %s!' % self)
        if self.jobmanager:
            self.log.info(
                'Processing finished tasks and terminating %s running tasks' % len(self.jobmanager.running_tasks))
            _process_finished_tasks(self.jobmanager)
            self.jobmanager.terminate()

        if due_to_failure:
            self.status = ExecutionStatus.failed
        else:
            self.status = ExecutionStatus.killed

    # @property
    # def tasksq(self):
    # stage_ids = [s.id for s in self.stages]
    # if len(stage_ids):
    # return self.session.query(Task).filter(Task.stage_id.in_(stage_ids))
    # else:
    # return []


    @property
    def tasks(self):
        return [t for s in self.stages for t in s.tasks]
        # return session.query(Task).join(Stage).filter(Stage.execution == ex).all()

    def stage_graph(self):
        """
        :return: (networkx.DiGraph) a DAG of the stages
        """
        g = nx.DiGraph()
        g.add_nodes_from(self.stages)
        g.add_edges_from((s, c) for s in self.stages for c in s.children if c)
        return g

    def task_graph(self):
        """
        :return: (networkx.DiGraph) a DAG of the tasks
        """
        g = nx.DiGraph()
        g.add_nodes_from(self.tasks)
        g.add_edges_from([(t, c) for t in self.tasks for c in t.children])
        return g

    def get_stage(self, name_or_id):
        if isinstance(name_or_id, int):
            f = lambda s: s.id == name_or_id
        else:
            f = lambda s: s.name == name_or_id

        for stage in self.stages:
            if f(stage):
                return stage

        raise ValueError('Stage with name %s does not exist' % name_or_id)

    @property
    def url(self):
        return url_for('cosmos.execution', name=self.name)

    def __repr__(self):
        return '<Execution[%s] %s>' % (self.id or '', self.name)

    def __unicode__(self):
        return self.__repr__()

    def delete(self, delete_files):
        """
        :param delete_files: (bool) If True, delete :attr:`output_dir` directory and all contents on the filesystem
        """
        if hasattr(self, 'log'):
            self.log.info('Deleting %s, output_dir=%s, delete_files=%s' % (self, self.output_dir, delete_files))
            for h in self.log.handlers:
                h.flush()
                h.close()
                self.log.removeHandler(h)
                # time.sleep(.1)  # takes a second for logs to flush?

        print >> sys.stderr, 'Deleting output_dir: %s...' % self.output_dir
        if delete_files and os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)

        ### Faster deleting can be done with explicit sql queries
        # from .TaskFile import InputFileAssociation
        # from .Task import TaskEdge
        # from .. import Stage, TaskFile
        # self.session.query(InputFileAssociation).join(Task).join(Stage).join(Execution).filter(Execution.id == self.id).delete()
        # self.session.query(TaskFile).join(Task).join(Stage).join(Execution).filter(Execution.id == self.id).delete()
        #
        # self.session.query(TaskEdge).join(Stage).join(Execution).filter(Execution.id == self.id).delete()
        # self.session.query(Task).join(Stage).join(Execution).filter(Execution.id == self.id).delete()
        # self.session.query(Stage).join(Execution).filter(Execution.id == self.id).delete()
        #
        print >> sys.stderr, '%s Deleting rom SQL...' % self
        self.session.delete(self)
        self.session.commit()
        print >> sys.stderr, '%s Deleted' % self

        # def yield_outputs(self, name):
        # for task in self.tasks:
        # tf = task.get_output(name, error_if_missing=False)
        # if tf is not None:
        #             yield tf
        #
        # def get_output(self, name):
        #     r = next(self.yield_outputs(name), None)
        #     if r is None:
        #         raise ValueError('Output named `{0}` does not exist in {1}'.format(name, self))
        #     return r


# @event.listens_for(Execution, 'before_delete')
# def before_delete(mapper, connection, target):
# print 'before_delete %s ' % target

def _run(execution, session, task_queue):
    """
    Do the execution!
    """
    execution.log.info('Executing TaskGraph')

    available_cores = True
    while len(task_queue) > 0:
        if available_cores:
            _run_queued_and_ready_tasks(task_queue, execution)
            available_cores = False

        for task in _process_finished_tasks(execution.jobmanager):
            if task.status == TaskStatus.failed and task.must_succeed:
                # pop all descendents when a task fails
                task_queue.remove_nodes_from(descendants(task_queue, task))
                task_queue.remove_node(task)
                execution.status = ExecutionStatus.failed_but_running
                execution.log.info('%s tasks left in the queue' % len(task_queue))
            elif task.status == TaskStatus.successful:
                # just pop this task
                task_queue.remove_node(task)
            elif task.status == TaskStatus.no_attempt:
                # the task must have failed, and is being reattempted
                pass
            else:
                raise AssertionError('Unexpected finished task status %s for %s' % (task.status, task))
            available_cores = True

        # only commit Task changes after processing a batch of finished ones
        session.commit()
        time.sleep(.3)


def _run_queued_and_ready_tasks(task_queue, execution):
    max_cpus = execution.max_cpus
    ready_tasks = [task for task, degree in task_queue.in_degree().items() if
                   degree == 0 and task.status == TaskStatus.no_attempt]

    if max_cpus is None:
        submittable_tasks = ready_tasks
    else:
        cores_used = sum([t.cpu_req for t in execution.jobmanager.running_tasks])
        cores_left = max_cpus - cores_used

        submittable_tasks = []
        ready_tasks = sorted(ready_tasks, key=lambda t: t.cpu_req)
        while len(ready_tasks) > 0:
            task = ready_tasks[0]
            there_are_cpus_left = task.cpu_req <= cores_left
            if there_are_cpus_left:
                cores_left -= task.cpu_req
                submittable_tasks.append(task)
                ready_tasks.pop(0)
            else:
                break

    # submit in a batch for speed
    execution.jobmanager.run_tasks(submittable_tasks)
    if len(submittable_tasks) < len(ready_tasks):
        execution.log.info('Reached max_cpus limit of %s, waiting for a task to finish...' % max_cpus)

    # only commit submitted Tasks after submitting a batch
    execution.session.commit()


def _process_finished_tasks(jobmanager):
    for task in jobmanager.get_finished_tasks():
        if task.NOOP or task.exit_status == 0:
            task.status = TaskStatus.successful
            yield task
        else:
            task.status = TaskStatus.failed
            yield task


def handle_exits(execution, do_atexit=True):
    # terminate on ctrl+c
    def ctrl_c(signal, frame):
        if not execution.successful:
            execution.log.info('Caught SIGINT (ctrl+c)')
            execution.terminate(due_to_failure=False)
            raise SystemExit('Execution terminated with a SIGINT (ctrl+c) event')

    signal.signal(signal.SIGINT, ctrl_c)

    if atexit:
        @atexit.register
        def cleanup_check():
            if execution.status == ExecutionStatus.running:
                execution.log.error('Execution %s has a status of running atexit!' % execution)
                execution.terminate(due_to_failure=True)
                # raise SystemExit('Execution terminated due to the python interpreter exiting')


def _copy_graph(graph):
    import networkx as nx

    graph2 = nx.DiGraph()
    graph2.add_edges_from(graph.edges())
    graph2.add_nodes_from(graph.nodes())
    return graph2
