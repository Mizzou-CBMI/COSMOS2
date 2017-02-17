from cosmos.api import Cosmos

def make_app(database_url):
    cosmos = Cosmos(database_url)
    flask = cosmos.init_flask()
    return flask
