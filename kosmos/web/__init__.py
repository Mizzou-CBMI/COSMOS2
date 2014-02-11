def runweb(host, port, database_url):
    '''start the flask development webserver'''

    from flask import Flask
    from views import gen_bprint
    import filters
    from flask.ext.sqlalchemy import SQLAlchemy

    app = Flask(__name__)
    app.config['SQLALCHEMY_DATABASE_URI'] = database_url
    db = SQLAlchemy(app)

    #print flask_app.url_map
    from . import views
    flask_app = Flask(__name__)
    flask_app.register_blueprint(gen_bprint(db.session))
    flask_app.config['DEBUG'] = True
    flask_app.secret_key = '\x07F\xdd\x98egfd\xc1\xe5\x9f\rv\xbe\xdbl\x93x\xc2\x19\x9e\xc0\xd7\xea'

    flask_app.run(debug=True, host=host, port=port)


