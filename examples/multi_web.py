from flask import Flask, request
from cosmos.api import Cosmos

flask_app = Flask(__name__)

from cosmos.web.views import gen_bprint

cosmos_bprint = gen_bprint()
flask_app.register_blueprint(cosmos_bprint, url_prefix='/<sqlite_db>/')


@cosmos_bprint.url_value_preprocessor
def get_profile_owner(endpoint, values):
    #query = User.query.filter_by(url_slug=values.pop('user_url_slug'))

    g.sqlite_db = values.pop('sqlite_db', None)


@flask_app.before_request
def test():
    print g.sqlite_db


# return self.flask_app.run(debug=debug, host=host, port=port)


flask_app.run('0.0.0.0', 2122)
