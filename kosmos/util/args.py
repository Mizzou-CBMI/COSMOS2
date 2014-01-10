def add_execution_args(parser):
    parser.add_argument('-n', '--name',
                        help="A name for this workflow", required=True)
    parser.add_argument('-q', '--default_queue', type=str,
                        help="Default queue.  Defaults to the value in cosmos.session.settings.")
    parser.add_argument('-o', '--output_dir', type=str, default=None,
                        help="The root output directory.  Output will be stored in root_output_dir/{workflow.name}.  "
                             "Defaults to the value in cosmos.session.settings.", required=True)
    parser.add_argument('-c', '--max_cores', type=int,
                        help="Maximum number (based on the sum of cpu_requirement) of cores to use at once.  0 means"
                             "unlimited", default=None)
    parser.add_argument('-r', '--restart', action='store_true',
                        help="Completely restart the workflow.  Note this will delete all records and output files of"
                             "the workflow being restarted!")
    parser.add_argument('-y', '--prompt_confirm', action='store_false',
                        help="Do not use confirmation prompts before restarting or deleting, and assume answer is"
                             "always yes.")


def parse_and_start(parser, session):
    parsed = parser.parse_args()
    kwargs = dict(parsed._get_kwargs())

    from kosmos import Execution
    d = { n:kwargs[n] for n in ['name', 'output_dir', 'restart'] }
    return Execution.start(session=session,**d), kwargs

def default_argparser(session=None):
    if session is None:
        from .. import get_session
        session = get_session()
    import argparse
    p = argparse.ArgumentParser()
    add_execution_args(p)
    return parse_and_start(p, session)