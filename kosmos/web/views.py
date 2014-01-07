from . import bprint
from flask import make_response, request, jsonify, abort, render_template, send_file
import io
from .. import Execution, Stage, Task
from ..db import session


@bprint.route('/')
def index():
    executions = session.query(Execution).all()
    return render_template('index.html', executions=executions)


@bprint.route('/execution/<int:id>/')
def execution(id):
    execution = session.query(Execution).get(id)
    return render_template('execution.html', execution=execution)


@bprint.route('/stage/<int:id>/')
def stage(id):
    stage = session.query(Stage).get(id)
    return render_template('stage.html', stage=stage)


@bprint.route('/task/<int:id>/')
def task(id):
    task = session.query(Task).get(id)
    return render_template('task.html', task=task,
                           stdout=open(task.output_stdout_path, 'r').read(),
                           stderr=open(task.output_stderr_path, 'r').read(),
                           command=open(task.output_command_script_path, 'r').read(),
    )


@bprint.route('/execution/<int:id>/taskgraph/')
def taskgraph(id):
    return render_template('taskgraph.html', execution=session.query(Execution).get(id))


# @bprint.route('/execution/<int:id>/taskgraph/svg/')
# def taskgraph_svg(id):
#     e = session.query(Execution).get(id)
#     tg = TaskGraph()
#     tg.from_tasks(e.tasks)
#     return send_file(io.BytesIO(tg.as_image()), mimetype='image/svg+xml')