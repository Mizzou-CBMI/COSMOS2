from flask import make_response, request, jsonify, Markup, render_template, Blueprint, redirect, url_for, flash
import io
from .. import Execution, Stage, Task, taskgraph as taskgraph_, TaskStatus
from ..job.JobManager import JobManager
from . import filters
from ..models.Recipe import stages_to_image
from sqlalchemy import desc
import itertools as it
from operator import attrgetter

def gen_bprint(cosmos_app):
    session = cosmos_app.session
    def get_execution(id):
        return session.query(Execution).filter_by(id=id).one()

    bprint = Blueprint('cosmos', __name__, template_folder='templates', static_folder='static',
                       static_url_path='/cosmos/static')
    filters.add_filters(bprint)

    @bprint.route('/execution/delete/<int:id>')
    def execution_delete(id):
        e = get_execution(id)
        e.delete(delete_files=True)
        flash('Deleted %s' % e)
        return redirect(url_for('cosmos.index'))

    @bprint.route('/')
    def index():
        executions = session.query(Execution).order_by(desc(Execution.created_on)).all()
        session.expire_all()
        return render_template('cosmos/index.html', executions=executions)


    @bprint.route('/execution/<int:id>/')
    def execution(id):
        execution = get_execution(id)
        return render_template('cosmos/execution.html', execution=execution)


    @bprint.route('/execution/<int:execution_id>/stage/<stage_name>/')
    def stage(execution_id, stage_name):
        stage = session.query(Stage).filter_by(execution_id=execution_id, name=stage_name).one()
        submitted = filter(lambda t: t.status == TaskStatus.submitted, stage.tasks)
        jm = JobManager(cosmos_app.get_submit_args)

        f = attrgetter('drm')
        drm_statuses = {}
        for drm, tasks in it.groupby(sorted(submitted, key=f), f):
            drm_statuses.update(jm.drms[drm].drm_statuses(list(tasks)))

        return render_template('cosmos/stage.html', stage=stage, drm_statuses=drm_statuses)
                               #x=filter(lambda t: t.status == TaskStatus.submitted, stage.tasks))


    @bprint.route('/execution/<int:ex_id>/stage/<stage_name>/delete/')
    def stage_delete(ex_id, stage_name):
        s = session.query(Stage).filter(Stage.execution_id == ex_id, Stage.name == stage_name).one()
        s.delete(delete_files=True)
        flash('Deleted %s' % s)
        return redirect(url_for('cosmos.execution', id=ex_id))

    @bprint.route('/task/<int:id>/')
    def task(id):
        task = session.query(Task).get(id)
        # resource_usage = [(category, field, getattr(task, field), profile_help[field]) for category, fields in
        #                   task.profile_fields for field in fields]
        resource_usage = [ (field, getattr(task, field)) for field in task.profile_fields ]
        return render_template('cosmos/task.html', task=task, resource_usage=resource_usage)


    @bprint.route('/execution/<int:id>/taskgraph/<type>/')
    def taskgraph(id, type):
        ex = get_execution(id)

        if type == 'task':
            svg = Markup(taskgraph_.tasks_to_image(ex.tasks))
        else:
            svg = Markup(stages_to_image(ex.stages))

        return render_template('cosmos/taskgraph.html', execution=ex, type=type,
                               svg=svg)

    # @bprint.route('/execution/<int:id>/taskgraph/svg/<type>/')
    # def taskgraph_svg(id, type, ):
    #     e = get_execution(id)
    #
    #     if type == 'task':
    #         return send_file(io.BytesIO(taskgraph_.tasks_to_image(e.tasks)), mimetype='image/svg+xml')
    #     else:
    #         return send_file(io.BytesIO(stages_to_image(e.stages)), mimetype='image/svg+xml')
    #
    return bprint


profile_help = dict(
    #time
    system_time='Amount of time that this process has been scheduled in kernel mode',
    user_time='Amount of time that this process has been scheduled in user mode.   This  includes  guest time,  guest_time  (time  spent  running a virtual CPU, see below), so that applications that are not aware of the guest time field do not lose that time from their calculations',
    cpu_time='system_time + user_time',
    wall_time='Elapsed real (wall clock) time used by the process.',
    percent_cpu='(cpu_time / wall_time) * 100',

    #memory
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

