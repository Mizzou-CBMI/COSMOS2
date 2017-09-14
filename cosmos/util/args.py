import sys


def get_last_cmd_executed():
    cmd_args = [a if ' ' not in a else "'" + a + "'" for a in sys.argv[1:]]
    return ' '.join([sys.argv[0]] + cmd_args)


def add_workflow_args(p, require_name=True):
    p.add_argument('--name', '-n', help="A name for this workflow", required=require_name)
    p.add_argument('--max_cores', '--max-cores', '-c', type=int,
                   help="Maximum number (based on the sum of Task.core_req) of cores to use at once.  0 means unlimited", default=None)
    p.add_argument('--restart', '-r', action='store_true',
                   help="Completely restart the workflow.  Note this will delete all record of the workflow in the database")
    p.add_argument('--skip_confirm', '--skip-confirm', '-y', action='store_true',
                   help="Do not use confirmation prompts before restarting or deleting, and assume answer is always yes")
    p.add_argument('--fail-fast', '--fail_fast', action='store_true',
                   help="terminate the entire workflow the first time a Task fails")
