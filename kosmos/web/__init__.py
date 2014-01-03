from flask import Flask, Blueprint

bprint = Blueprint('kosmos', __name__, template_folder='templates',static_folder='static')

def runweb(host, port):
    '''start the flask development webserver'''
    flask_app = Flask(__name__)
    flask_app.register_blueprint(bprint)
    print flask_app.url_map
    flask_app.run(debug=True, host=host, port=port)

from . import views

