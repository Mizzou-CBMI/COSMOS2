from flask import Flask
from views import bprint
import filters

def runweb(host, port):
    '''start the flask development webserver'''
    #print flask_app.url_map
    from . import views
    flask_app = Flask(__name__)
    flask_app.register_blueprint(bprint)
    flask_app.config['DEBUG'] = True

    # Remove polymorphic mapping for the website
    from .. import Task
    Task.__mapper__.polymorphic_on = None
    flask_app.run(debug=True, host=host, port=port)


