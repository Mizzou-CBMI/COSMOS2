import itertools as it
from operator import attrgetter

from flask import Markup, render_template, Blueprint, redirect, url_for, flash, abort, request
from sqlalchemy import desc

from cosmos.api import Workflow, Stage, Task, TaskStatus
from ..job.JobManager import JobManager
from . import filters
from ..graph.draw import draw_task_graph, draw_stage_graph


def gen_bprint(cosmos_app):
    session = cosmos_app.session

    def get_workflow(id):
        return session.query(Workflow).filter_by(id=id).one()

    bprint = Blueprint('cosmos', __name__, template_folder='templates', static_folder='static',
                       static_url_path='/cosmos/static')
    filters.add_filters(bprint)

    @bprint.route('/workflow/delete/<int:id>')
    def workflow_delete(id):
        e = get_workflow(id)
        e.delete(delete_files=True)
        flash('Deleted %s' % e)
        return redirect(url_for('cosmos.index'))

    @bprint.route('/')
    def index():
        workflows = session.query(Workflow).order_by(desc(Workflow.created_on)).all()
        session.expire_all()
        return render_template('cosmos/index.html', workflows=workflows)

    @bprint.route('/')
    def home():
        return index()

    @bprint.route('/workflow/<name>/')
    # @bprint.route('/workflow/<int:id>/')
    def workflow(name):
        workflow = session.query(Workflow).filter_by(name=name).one()
        return render_template('cosmos/workflow.html', workflow=workflow)


    @bprint.route('/workflow/<workflow_name>/<stage_name>/')
    def stage(workflow_name, stage_name):
        ex = session.query(Workflow).filter_by(name=workflow_name).one()
        stage = session.query(Stage).filter_by(workflow_id=ex.id, name=stage_name).one()
        if stage is None:
            return abort(404)
        submitted = filter(lambda t: t.status == TaskStatus.submitted, stage.tasks)
        jm = JobManager(cosmos_app, cosmos_app.get_submit_args)

        f = attrgetter('drm')
        drm_statuses = {}
        for drm, tasks in it.groupby(sorted(submitted, key=f), f):
            drm_statuses.update(jm.get_drm(drm).drm_statuses(list(tasks)))

        return render_template('cosmos/stage.html', stage=stage, drm_statuses=drm_statuses)
        # x=filter(lambda t: t.status == TaskStatus.submitted, stage.tasks))


    @bprint.route('/workflow/<int:ex_id>/stage/<stage_name>/delete/<int:delete_descendants>')
    def stage_delete(ex_id, stage_name, delete_descendants):
        assert delete_descendants in [0, 1]
        delete_descendants = bool(delete_descendants)
        s = session.query(Stage).filter(Stage.workflow_id == ex_id, Stage.name == stage_name).one()
        flash('Deleted %s' % s)
        ex_url = s.workflow.url
        s.delete(delete_files=False, delete_descendants=delete_descendants)
        return redirect(ex_url)

    # @bprint.route('/task/<int:id>/')
    # def task(id):
    # task = session.query(Task).get(id)
    # if task is None:
    #         return abort(404)
    #     return redirect(url_for('cosmos.task_friendly', ex_name=task.workflow.name, stage_name=task.stage.name, task_id=task.id))

    # @bprint.route('/workflow/<ex_name>/<stage_name>/task/')
    # def task(ex_name, stage_name):
    #     # resource_usage = [(category, field, getattr(task, field), profile_help[field]) for category, fields in
    #     #                   task.profile_fields for field in fields]
    #     assert request.method == 'GET'
    #     params = request.params
    #     ex = session.query(Workflow).filter_by(name=ex_name).one()
    #     stage = session.query(Stage).filter_by(workflow=ex, name=stage_name).one()
    #     task = session.query(Task).filter_by(stage=stage, params=params).one()
    #     if task is None:
    #         return abort(404)
    #     resource_usage = [(field, getattr(task, field)) for field in task.profile_fields]
    #     return render_template('cosmos/task.html', task=task, resource_usage=resource_usage)

    @bprint.route('/workflow/<ex_name>/<stage_name>/task/<task_id>')
    def task(ex_name, stage_name, task_id):
        # resource_usage = [(category, field, getattr(task, field), profile_help[field]) for category, fields in
        #                   task.profile_fields for field in fields]
        task = session.query(Task).get(task_id)
        if task is None:
            return abort(404)
        resource_usage = [(field, getattr(task, field)) for field in task.profile_fields]
        return render_template('cosmos/task.html', task=task, resource_usage=resource_usage)

    @bprint.route('/workflow/<int:id>/taskgraph/<type>/')
    def taskgraph(id, type):
        from ..graph.draw import pygraphviz_available
        ex = get_workflow(id)

        if pygraphviz_available:
            if type == 'task':
                svg = Markup(draw_task_graph(ex.task_graph(), url=True))
            else:
                svg = Markup(draw_stage_graph(ex.stage_graph(), url=True))
        else:
            svg = 'Pygraphviz not installed, cannot visualize.  (Usually: apt-get install graphviz && pip install pygraphviz)'

        return render_template('cosmos/taskgraph.html', workflow=ex, type=type,
                               svg=svg)

    # @bprint.route('/workflow/<int:id>/taskgraph/svg/<type>/')
    # def taskgraph_svg(id, type, ):
    #     e = get_workflow(id)
    #
    #     if type == 'task':
    #         return send_file(io.BytesIO(taskgraph_.tasks_to_image(e.tasks)), mimetype='image/svg+xml')
    #     else:
    #         return send_file(io.BytesIO(stages_to_image(e.stages)), mimetype='image/svg+xml')
    #
    return bprint


profile_help = dict(
    # time
    system_time='Amount of time that this process has been scheduled in kernel mode',
    user_time='Amount of time that this process has been scheduled in user mode.   This  includes  guest time,  guest_time  (time  spent  running a virtual CPU, see below), so that applications that are not aware of the guest time field do not lose that time from their calculations',
    cpu_time='system_time + user_time',
    wall_time='Elapsed real (wall clock) time used by the process.',
    percent_cpu='(cpu_time / wall_time) * 100',

    # memory
    avg_rss_mem='Average resident set size (Kb)',
    max_rss_mem='Maximum resident set size (Kb)',
    single_proc_max_peak_rss='Maximum single process rss used (Kb)',
    avg_virtual_mem='Average virtual memory used (Kb)',
    max_virtual_mem='Maximum virtual memory used (Kb)',
    single_proc_max_peak_virtual_mem='Maximum single process virtual memory used (Kb)',
    major_page_faults='The number of major faults the process has made which have required loading a memory page from disk',
    minor_page_faults='The number of minor faults the process has made which have not required loading a memory page from disk',
    avg_data_mem='Average size of data segments (Kb)',
    max_data_mem='Maximum size of data segments (Kb)',
    avg_lib_mem='Average library memory size (Kb)',
    max_lib_mem='Maximum library memory size (Kb)',
    avg_locked_mem='Average locked memory size (Kb)',
    max_locked_mem='Maximum locked memory size (Kb)',
    avg_num_threads='Average number of threads',
    max_num_threads='Maximum number of threads',
    avg_pte_mem='Average page table entries size (Kb)',
    max_pte_mem='Maximum page table entries size (Kb)',

    #io
    nonvoluntary_context_switches='Number of non voluntary context switches',
    voluntary_context_switches='Number of voluntary context switches',
    block_io_delays='Aggregated block I/O delays',
    avg_fdsize='Average number of file descriptor slots allocated',
    max_fdsize='Maximum number of file descriptor slots allocated',

    #misc
    num_polls='Number of times the resource usage statistics were polled from /proc',
    names='Names of all descendnt processes (there is always a python process for the profile_working.py script)',
    num_processes='Total number of descendant processes that were spawned',
    pids='Pids of all the descendant processes',
    exit_status='Exit status of the primary process being profiled',
    SC_CLK_TCK='sysconf(_SC_CLK_TCK), an operating system variable that is usually equal to 100, or centiseconds',
)

