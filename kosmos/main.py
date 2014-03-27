# from . import web, db, KosmosApp
#
#
# def shell(kosmos_app):
#     import IPython
#     from . import rel, Recipe, TaskFile, Task, Inputs, rel, Stage, Execution, TaskStatus, StageStatus, Tool
#
#     executions = session.query(Execution).all()
#     ex = session.query(Execution).first()
#     stage = session.query(Stage).first()
#     task = session.query(Task).first()
#     IPython.embed()
#
#
# def parse_args():
#     import os
#
#     import argparse
#
#     parser = argparse.ArgumentParser(prog='kosmos', description=__doc__,
#                                      formatter_class=argparse.ArgumentDefaultsHelpFormatter)
#     sps = parser.add_subparsers()
#
#     sp = sps.add_parser('shell', help=shell.__doc__)
#     sp.add_argument('-d', '--database_url', help='sqlalchemy database_url')
#     sp.set_defaults(func=shell)
#     args = parser.parse_args()
#     kwargs = dict(args._get_kwargs())
#     del kwargs['func']
#
#     debug = kwargs.pop('debug', False)
#     if args.func.__name__ in ['shell']:
#         from kosmos import KosmosApp
#
#         db_url = kwargs.pop('database_url')
#         kosmos = KosmosApp(db_url)
#         kwargs['session'] = kosmos.sqla.session
#     if debug:
#         import ipdb
#
#         with ipdb.launch_ipdb_on_exception():
#             args.func(**kwargs)
#     else:
#         args.func(**kwargs)
#
