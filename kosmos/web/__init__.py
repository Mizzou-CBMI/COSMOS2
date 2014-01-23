def runweb(host, port, database_url):
    '''start the flask development webserver'''

    from flask import Flask
    from views import gen_bprint
    import filters

    #print flask_app.url_map
    from . import views
    from .. import get_session
    flask_app = Flask(__name__)
    flask_app.register_blueprint(gen_bprint(get_session(database_url)))
    flask_app.config['DEBUG'] = True

    # Remove polymorphic mapping for the website
    from .. import Task
    Task.__mapper__.polymorphic_on = None
    flask_app.run(debug=True, host=host, port=port)


