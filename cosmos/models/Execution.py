import os
import re
import shutil
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.schema import Column
from sqlalchemy.types import Boolean, Integer, String, DateTime, VARCHAR
from sqlalchemy import orm
from sqlalchemy.sql.expression import func
from sqlalchemy.orm import validates, synonym
from flask import url_for
import networkx as nx
from networkx.algorithms.dag import descendants, topological_sort
import atexit

from ..util.helpers import duplicates
from ..graph import taskgraph
from ..db import Base
import time

opj = os.path.join
import signal

from .. import TaskStatus, StageStatus, Task, ExecutionStatus, signal_execution_status_change

from ..util.helpers import get_logger, mkdir, confirm
from ..util.sqla import Enum34_ColumnType, MutableDict, JSONEncodedDict
from ..util.args import get_last_cmd_executed


def _default_task_log_output_dir(task):
    """The default function for computing Task.log_output_dir"""
    return opj(task.execution.output_dir, 'log', task.stage.name, str(task.id))


def _default_task_output_dir(task):
    """The default function for computing Task.output_dir"""
    tag_values = map(str, task.tags.values())
    for v in tag_values:
        assert re.match("^[\w]+$", v), 'tag value `%s` does not make a good directory name.  Either change the tag, or define your own task_output_dir function when calling' \
                                       'Execution.run' % v
    return opj(task.execution.output_dir, task.stage.name, '__'.join(tag_values))


@signal_execution_status_change.connect
def _execution_status_changed(ex):
    if ex.status in [ExecutionStatus.successful, ExecutionStatus.failed, ExecutionStatus.killed]:
        ex.log.info('%s %s, output_dir: %s' % (ex, ex.status, ex.output_dir))
        ex.finished_on = func.now()

    if ex.status == ExecutionStatus.successful:
        ex.successful = True
        ex.finished_on = func.now()

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
    created_on = Column(DateTime, default=func.now())
    started_on = Column(DateTime)
    finished_on = Column(DateTime)
    max_cpus = Column(Integer)
    max_attempts = Column(Integer, default=1)
    info = Column(MutableDict.as_mutable(JSONEncodedDict))
    # recipe_graph = Column(PickleType)
    _status = Column(Enum34_ColumnType(ExecutionStatus), default=ExecutionStatus.no_attempt)

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
        assert re.match(r"^[\.\w-]+$", name), 'Invalid execution name.'
        return name

    @classmethod
    def start(cls, cosmos_app, name, output_dir, restart=False, skip_confirm=False, max_cpus=None, max_attempts=1, output_dir_exists_error=True):
        """
        Start, resume, or restart an execution based on its name and the session.  If resuming, deletes failed tasks.

        :param session: (sqlalchemy.session)
        :param name: (str) a name for the workflow.
        :param output_dir: (str) the directory to write files to
        :param restart: (bool) if True and the execution exists, delete it first
        :param skip_confirm: (bool) if True, do not prompt the shell for input before deleting executions or files
        :param max_cpus: (int) the maximum number of CPUs to use at once.  Based on the sum of the running tasks' task.cpu_req

        :returns: an instance of Execution
        """
        assert os.path.exists(os.getcwd()), 'The current working dir, %s, does not exist' % os.getcwd()
        output_dir = os.path.abspath(output_dir)
        session = cosmos_app.session
        # assert name is not None, 'name cannot be None'
        assert output_dir is not None, 'output_dir cannot be None'
        output_dir = output_dir if output_dir[-1] != '/' else output_dir[0:]  # remove trailing slash
        prefix_dir = os.path.split(output_dir)[0]
        assert os.path.exists(prefix_dir), '%s does not exists' % prefix_dir

        old_id = None
        if restart:
            ex = session.query(Execution).filter_by(name=name).first()
            if ex:
                old_id = ex.id
                msg = 'Restarting %s.  Are you sure you want to delete the contents of output_dir `%s` and all sql records for this execution?' % (ex.output_dir, ex)
                if not skip_confirm and not confirm(msg):
                    raise SystemExit('Quitting')

                ex.delete(delete_files=True)
            else:
                if not skip_confirm and not confirm('Execution with name %s does not exist, but `restart` is set to True.  Continue by starting a new Execution?' % name):
                    raise SystemExit('Quitting')

        # resuming?
        ex = session.query(Execution).filter_by(name=name).first()
        # msg = 'Execution started, Cosmos v%s' % __version__
        if ex:
            # resuming.
            if not skip_confirm and not confirm(
                            'Resuming %s.  All non-successful jobs will be deleted, then any new tasks in the graph will be added and executed.  Are you sure?' % ex):
                raise SystemExit('Quitting')
            ex.successful = False
            ex.finished_on = None
            if output_dir is None:
                output_dir = ex.output_dir
            else:
                assert ex.output_dir == output_dir, 'cannot change the output_dir of an execution being resumed.'

            if not os.path.exists(ex.output_dir):
                raise IOError('output_directory %s does not exist, cannot resume %s' % (ex.output_dir, ex))

            ex.log.info('Resuming %s' % ex)
            session.add(ex)
            failed_tasks = [t for s in ex.stages for t in s.tasks if not t.successful]
            n = len(failed_tasks)
            if n:
                ex.log.info('Deleting %s failed task(s), delete_files=%s' % (n, False))
                # stages_with_failed_tasks = set()
                for t in failed_tasks:
                    session.delete(t)
                    #stages_with_failed_tasks.add(t.stage)
            stages = filter(lambda s: len(s.tasks) == 0, ex.stages)
            for stage in stages:
                ex.log.info('Deleting stage %s, since it has no successful Tasks' % stage)
                session.delete(stage)

        else:
            # start from scratch
            if output_dir_exists_error:
                assert not os.path.exists(output_dir), 'Execution output_dir `%s` already exists.' % (output_dir)
            ex = Execution(id=old_id, name=name, output_dir=output_dir, manual_instantiation=False)
            # ex.log.info(msg)
            session.add(ex)

        ex.max_cpus = max_cpus
        ex.max_attempts = max_attempts
        ex.info['last_cmd_executed'] = get_last_cmd_executed()
        session.commit()
        session.expunge_all()
        session.add(ex)
        mkdir(output_dir)
        ex.cosmos_app = cosmos_app

        return ex

    @orm.reconstructor
    def constructor(self):
        self.__init__(manual_instantiation=False)

    def __init__(self, manual_instantiation=True, *args, **kwargs):
        if manual_instantiation:
            raise TypeError, 'Do not instantiate an Execution manually.  Use the Execution.start staticmethod.'
        super(Execution, self).__init__(*args, **kwargs)
        assert self.output_dir is not None, 'output_dir cannot be None'
        if self.info is None:
            # mutable dict column defaults to None
            self.info = dict()
        self.jobmanager = None

    def __getattr__(self, item):
        if item == 'log':
            self.log = get_logger('cosmos-%s' % Execution.name, opj(self.output_dir, 'execution.log'))
            return self.log
        else:
            raise AttributeError


    def run(self, recipe, task_output_dir=_default_task_output_dir, log_output_dir=_default_task_log_output_dir, dry=False, set_successful=True):
        """
        Renders and executes the :param:`recipe`

        :param recipe: (Recipe) the Recipe to render and execute.
        :param task_output_dir: (function) a function that computes a task's output_dir. It receives one parameter: the task instance.  By default task output is stored in
            output_dir/stage_name/'_'.join(task.tags.values()).  See _default_task_log_output_dir for more info.
        :param log_output_dir: (function) a function that computes a task's log_output_dir.  It receives one parameter: the task instance.
             By default task log output is stored in output_dir/log/stage_name/task_id.  See _default_task_log_output_dir for more info.
        :param dry: (bool) if True, do not actually run any jobs.
        :param set_successful: (bool) sets this execution as successful if all rendered recipe executes without a failure.  You might set this to False if you intend to add and
            run more tasks in this execution later.

        """
        assert os.path.exists(os.getcwd()), 'current working dir does not exist! %s' % os.getcwd()
        assert hasattr(self, 'cosmos_app'), 'Execution was not initialized using the Execution.start method'
        assert hasattr(task_output_dir, '__call__'), 'task_output_dir must be a function'
        assert hasattr(log_output_dir, '__call__'), 'log_output_dir must be a function'
        assert self.session, 'Execution must be part of a sqlalchemy session'
        session = self.session

        from ..job.JobManager import JobManager

        self.jobmanager = JobManager(get_submit_args=self.cosmos_app.get_submit_args, default_queue=self.cosmos_app.default_queue)

        self.log.info('Rendering taskgraph for %s using DRM `%s`, output_dir: `%s`' % (self, self.cosmos_app.default_drm, self.output_dir))
        self.status = ExecutionStatus.running
        self.successful = False

        if self.started_on is None:
            self.started_on = func.now()

        # Render task graph and to session
        task_g, stage_g = taskgraph.render_recipe(self, recipe, default_drm=self.cosmos_app.default_drm)

        # Set output_dirs of new tasks
        for task in nx.topological_sort(task_g):
            if not task.successful:
                task.output_dir = task_output_dir(task)
                assert task.output_dir not in ['', None], "Computed an output file root_path of None or '' for %s" % task
                for tf in task.output_files:
                    if tf.path is None:
                        tf.path = opj(task.output_dir, tf.basename)
                        assert tf.path is not None, 'computed an output_dir for %s of None' % task
                        # recipe_stage2stageprint task, tf.root_path, 'basename:',tf.basename

        # set commands of new tasks
        for task in topological_sort(task_g):
            if not task.successful and not task.NOOP:
                task.command = task.tool._generate_command(task)

        # Assert no duplicate TaskFiles
        import itertools as it

        taskfiles = (tf for task in task_g.nodes() for tf in task.output_files)
        f = lambda tf: tf.path
        for path, group in it.groupby(sorted(filter(lambda tf: not tf.task_output_for.NOOP, taskfiles), key=f), f):
            group = list(group)
            if len(group) > 1:
                raise ValueError('Duplicate taskfiles paths detected:\n %s.%s\n %s.%s' % (group[0].task_output_for, group[0], group[1].task_output_for, group[1]))


        # Collapse
        from ..graph.collapse import collapse

        for stage_bubble, name in recipe.collapses:
            self.log.debug('Collapsing %s into `%s`' % ([s.name for s in stage_bubble], name))
            collapse(session, task_g, stage_g, stage_bubble, name)

        # taskg and stageg are now finalized

        stages = stage_g.nodes()
        assert len(set(stages)) == len(stages), 'duplicate stage name detected: %s' % (next(duplicates(stages)))

        # renumber stages
        for i, s in enumerate(topological_sort(stage_g)):
            s.number = i + 1

        # Add final taskgraph to session
        session.expunge_all()
        session.add(self)
        session.add_all(stage_g.nodes())
        session.add_all(task_g.nodes())
        successful = filter(lambda t: t.successful, task_g.nodes())

        # commit so task.id is set for log dir
        self.log.info('Committing %s Tasks to the SQL database...' % (len(task_g.nodes()) - len(successful)))
        session.commit()

        # print stages
        for s in topological_sort(stage_g):

            self.log.info('%s %s' % (s, s.status))

        # Create Task Queue
        task_queue = _copy_graph(task_g)
        self.log.info('Skipping %s successful tasks' % len(successful))
        task_queue.remove_nodes_from(successful)


        handle_exits(self)


        self.log.info('Setting log output directories...')
        # set log dirs
        log_dirs = {t.log_dir: t for t in successful}
        for task in task_queue.nodes():
            log_dir = log_output_dir(task)
            assert log_dir not in log_dirs, 'Duplicate log_dir detected for %s and %s' % (task, log_dirs[log_dir])
            log_dirs[log_dir] = task
            task.log_dir = log_dir

        self.log.info('Resetting stage attributes...')
        def reset_stage_attrs():
            """Update stage attributes if new tasks were added to them"""
            from .. import Stage, StageStatus
            # using .update() threw an error, so have to do it the slow way. It's not too bad though, since
            # there shouldn't be that many stages to update.
            for s in session.query(Stage).join(Task).filter(~Task.successful, Stage.execution_id == self.id, Stage.status != StageStatus.no_attempt):
                s.successful = False
                s.finished_on = None
                s.status = StageStatus.running

        reset_stage_attrs()

        self.log.info('Ensuring there are enough cores...')
        # make sure we've got enough cores
        for t in task_queue:
            assert t.cpu_req <= self.max_cpus or self.max_cpus is None, '%s requires more cpus (%s) than `max_cpus` (%s)' % (t, t.cpu_req, self.max_cpus)

        #Run this thing!
        if not dry:
            _run(self, session, task_queue)

        # set status
        if self.status == ExecutionStatus.failed_but_running:
            self.status = ExecutionStatus.failed
            return False
        elif self.status == ExecutionStatus.running:
            if set_successful:
                self.status = ExecutionStatus.successful
            return True
        else:
            raise AssertionError('Bad execution status %s' % self.status)

        # set stage status to failed
        for s in self.stages:
            if s.status in [StageStatus.running, StageStatus.running_but_failed]:
                s.status = StageStatus.failed

    def terminate(self, due_to_failure=True):
        self.log.warning('Terminating!')
        if self.jobmanager:
            self.log.info('Cleaning up and terminating %s running tasks' % len(self.jobmanager.running_tasks))
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
    #     else:
    #         return []


    @property
    def tasks(self):
        return [t for s in self.stages for t in s.tasks]
        # return session.query(Task).join(Stage).filter(Stage.execution == ex).all()

    @property
    def taskfilesq(self):
        from . import TaskFile, Stage

        return self.session.query(TaskFile).join(Task, Stage, Execution).filter(Execution.id == self.id)

    def stage_graph(self):
        """
        :return: (networkx.DiGraph) a DAG of the stages
        """
        g = nx.DiGraph()
        g.add_nodes_from(self.stages)
        g.add_edges_from((s, c) for s in self.stages for c in s.children)
        return g

    def draw_stage_graph(self):
        from ..graph.draw import draw_stage_graph

        return draw_stage_graph(self.stage_graph())

    def task_graph(self):
        """
        :return: (networkx.DiGraph) a DAG of the tasks
        """
        g = nx.DiGraph()
        g.add_nodes_from(self.tasks)
        g.add_edges_from([(t, c) for t in self.tasks for c in t.children])
        return g

    def draw_task_graph(self):
        from ..graph.draw import draw_task_graph

        return draw_task_graph(self.task_graph())


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
            self.log.info('Deleting %s, delete_files=%s' % (self, delete_files))
            for h in self.log.handlers:
                h.flush()
                h.close()
                self.log.removeHandler(h)
                # time.sleep(.1)  # takes a second for logs to flush?
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
        self.session.delete(self)
        self.session.commit()

        # def yield_outputs(self, name):
        # for task in self.tasks:
        # tf = task.get_output(name, error_if_missing=False)
        #         if tf is not None:
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
    ready_tasks = [task for task, degree in task_queue.in_degree().items() if degree == 0 and task.status == TaskStatus.no_attempt]
    for ready_task in sorted(ready_tasks, key=lambda t: t.cpu_req):
        cores_used = sum([t.cpu_req for t in execution.jobmanager.running_tasks])
        if max_cpus is not None and ready_task.cpu_req + cores_used > max_cpus:
            execution.log.info('Reached max_cpus limit of %s, waiting for a task to finish...' % max_cpus)
            break

        # # # render taskfile paths
        # for f in ready_task.output_files:
        # if f.root_path is None:
        #         f.root_path = os.root_path.join(ready_task.output_dir, f.basename)
        execution.jobmanager.submit(ready_task)

    # only commit submitted Tasks after submitting a batch
    execution.session.commit()


def _process_finished_tasks(jobmanager):
    for task in jobmanager.get_finished_tasks():
        if task.NOOP or task.profile.get('exit_status', None) == 0:
            task.status = TaskStatus.successful
            yield task
        else:
            task.status = TaskStatus.failed
            yield task


def handle_exits(execution):
    # terminate on ctrl+c
    def ctrl_c(signal, frame):
        if not execution.successful:
            execution.log.info('Caught SIGINT (ctrl+c)')
            execution.terminate(due_to_failure=False)
            raise SystemExit('Execution terminated with a SIGINT (ctrl+c) event')
    signal.signal(signal.SIGINT, ctrl_c)

    @atexit.register
    def cleanup_check():
        if execution.status not in [ExecutionStatus.failed, ExecutionStatus.killed, ExecutionStatus.no_attempt]:
            execution.log.info('Initiating atexit termination')
            execution.terminate(due_to_failure=False)
            raise SystemExit('Execution terminated due to the python interpreter exiting')



def _copy_graph(graph):
    import networkx as nx

    graph2 = nx.DiGraph()
    graph2.add_edges_from(graph.edges())
    graph2.add_nodes_from(graph.nodes())
    return graph2