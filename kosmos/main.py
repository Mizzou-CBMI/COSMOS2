from . import web, db


def shell(database_url=None):
    import IPython
    from kosmos import *
    from kosmos.db import get_session

    session = get_session(database_url=database_url)

    Task.__mapper__.polymorphic_on = None

    executions = session.query(Execution).all()
    ex = session.query(Execution).first()
    s = session.query(Stage).first()
    t = session.query(Task).first()
    IPython.embed()


def parse_args():
    import argparse
    from . import config

    parser = argparse.ArgumentParser(prog='kosmos', description=__doc__,
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    sps = parser.add_subparsers()

    sp = sps.add_parser('initdb', help=db.initdb.__doc__)
    sp.add_argument('-d', '--database_url', help='sqlalchemy database_url, default is %s' % config['database_url'],
                    default=config['database_url'])
    sp.set_defaults(func=db.initdb)

    sp = sps.add_parser('resetdb', help=db.resetdb.__doc__)
    sp.add_argument('-d', '--database_url', help='sqlalchemy database_url, default is %s' % config['database_url'],
                    default=config['database_url'])
    sp.set_defaults(func=db.resetdb)

    sp = sps.add_parser('shell', help=shell.__doc__)
    sp.add_argument('-d', '--database_url', help='sqlalchemy database_url, default is %s' % config['database_url'],
                    default=config['database_url'])
    sp.set_defaults(func=shell)

    sp = sps.add_parser('runweb', help=web.runweb.__doc__)
    sp.add_argument('-p', '--port', type=int, default=4848,
                    help='port to bind the server to')
    sp.add_argument('-H', '--host', default='localhost',
                    help='host to bind the server to')
    sp.add_argument('-d', '--database_url', help='sqlalchemy database_url, default is %s, which can be set by altering'
                                                 '~/.kosmos/kosmos.conf' % config['database_url'],
                    default=config['database_url'])
    sp.set_defaults(func=web.runweb)

    args = parser.parse_args()
    kwargs = dict(args._get_kwargs())
    del kwargs['func']

    debug = kwargs.pop('debug', False)
    if debug:
        import ipdb

        with ipdb.launch_ipdb_on_exception():
            args.func(**kwargs)
    else:
        args.func(**kwargs)

