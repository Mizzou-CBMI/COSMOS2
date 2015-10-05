#!/usr/bin/env python
"""
An example of how to setup a project of multiple workflows
"""
import os

from cosmos.api import Execution, add_execution_args, Cosmos
from configparser import ConfigParser
from cosmos.util.helpers import mkdir

root_path = os.path.dirname(os.path.realpath(__file__))
config = ConfigParser()
config.read(os.path.join(root_path, 'settings.conf'))
settings = config['main']

if __name__ == '__main__':
    import ex1
    import ex_fail
    import ex_email

    cosmos = Cosmos('sqlite:///%s/sqlite.db' % os.path.dirname(os.path.abspath(__file__)))
    cosmos.initdb()

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('-g', '--growl', action='store_true',
                        help='sends a growl notification on execution status changes')
    parser.add_argument('-d', '--debug', action='store_true',
                        help='launch ipdb on exception')
    sps = parser.add_subparsers(title="Commands", metavar="<command>")

    sp = sps.add_parser('resetdb', help=cosmos.resetdb.__doc__)
    sp.set_defaults(func=cosmos.resetdb)

    sp = sps.add_parser('initdb', help=cosmos.initdb.__doc__)
    sp.set_defaults(func=cosmos.initdb)

    sp = sps.add_parser('shell', help=cosmos.shell.__doc__)
    sp.set_defaults(func=cosmos.shell)

    sp = sps.add_parser('runweb', help=cosmos.runweb.__doc__)
    sp.add_argument('-p', '--port', type=int, help='port to bind the server to')
    sp.add_argument('-H', '--host', default='localhost', help='host to bind the server to')
    sp.set_defaults(func=cosmos.runweb)

    sp = sps.add_parser('ex1', help='Example1')
    sp.set_defaults(func=ex1.run_ex1)
    add_execution_args(sp)

    sp = sps.add_parser('ex2', help='Example2: A failed task')
    sp.set_defaults(func=ex_fail.run_ex2)
    add_execution_args(sp)

    sp = sps.add_parser('ex3', help='Example3: Twitter (note you must edit the file)')
    sp.set_defaults(func=ex_email.run_ex3)
    add_execution_args(sp)

    args = parser.parse_args()
    kwargs = dict(args._get_kwargs())
    func = kwargs.pop('func')
    growl = kwargs.pop('growl')
    debug = kwargs.pop('debug')
    if growl:
        from cosmos.util import growl
        from cosmos.api import signal_execution_status_change, ExecutionStatus

        @signal_execution_status_change.connect
        def growl_signal(execution):
            if execution.status != ExecutionStatus.running:
                growl.send('%s %s' % (execution, execution.status))

    if func.__module__.startswith('ex'):
        execution_params = {n: kwargs.pop(n, None) for n in
                            ['name', 'restart', 'skip_confirm', 'max_cpus', 'max_attempts', 'output_dir']}
        if not execution_params['output_dir']:
            mkdir(os.path.join(root_path, 'out_dir'))
            execution_params['output_dir'] = os.path.join(root_path, 'out_dir', execution_params['name'])

        ex = cosmos.start(**execution_params)
        kwargs['execution'] = ex

    if debug:
        import ipdb
        with ipdb.launch_ipdb_on_exception():
            func(**kwargs)
    else:
        func(**kwargs)
