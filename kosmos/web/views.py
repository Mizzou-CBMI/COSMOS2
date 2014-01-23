from flask import make_response, request, jsonify, abort, render_template, send_file, Blueprint
import io
from .. import Execution, Stage, Task, taskgraph as taskgraph_
from . import filters

def gen_bprint(session):

    bprint = Blueprint('kosmos', __name__, template_folder='templates', static_folder='static')
    filters.add_filters(bprint)

    @bprint.route('/')
    def index():
        executions = session.query(Execution).all()
        return render_template('index.html', executions=executions)


    @bprint.route('/execution/<int:id>/')
    def execution(id):
        execution = session.query(Execution).get(id)
        return render_template('execution.html', execution=execution)


    @bprint.route('/execution/<int:execution_id>/stage/<stage_name>/')
    def stage(execution_id, stage_name):
        stage = session.query(Stage).filter_by(execution_id=execution_id, name=stage_name).one()
        return render_template('stage.html', stage=stage)


    @bprint.route('/task/<int:id>/')
    def task(id):
        task = session.query(Task).get(id)
        resource_usage = [(category, field, getattr(task, field), profile_help[field]) for category, fields in
                          task.profile_fields for field in fields]
        return render_template('task.html', task=task, resource_usage=resource_usage)


    @bprint.route('/execution/<int:id>/taskgraph/<type>/')
    def taskgraph(id, type):
        return render_template('taskgraph.html', execution=session.query(Execution).get(id), type=type)


    @bprint.route('/execution/<int:id>/taskgraph/svg/<type>/')
    def taskgraph_svg(id, type):
        e = session.query(Execution).get(id)

        if type == 'task':
            dag = taskgraph_.dag_from_tasks(e.tasks)
            return send_file(io.BytesIO(taskgraph_.as_image(dag)), mimetype='image/svg+xml')
        else:
            return send_file(io.BytesIO(e.recipe_graph), mimetype='image/svg+xml')

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
    names='Names of all descendnt processes (there is always a python process for the profile.py script)',
    num_processes='Total number of descendant processes that were spawned',
    pids='Pids of all the descendant processes',
    exit_status='Exit status of the primary process being profiled',
    SC_CLK_TCK='sysconf(_SC_CLK_TCK), an operating system variable that is usually equal to 100, or centiseconds',
)

