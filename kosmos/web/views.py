from . import bprint
from flask import make_response, request, jsonify, abort, render_template
from .. import Execution
from ..db import session

@bprint.route('/')
def index():
    executions = session.query(Execution).all()
    return render_template('index.html', executions = executions)
