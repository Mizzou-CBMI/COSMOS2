from kosmos import Execution, Kosmos, rel, Recipe, Input
import tools


def get_submit_args(drm, task):
    """
    Default method for determining the arguments to pass to the drm specified by :param:`drm`

    :returns: (str) arguments.  For example, returning "-n 3" if :param:`drm` == 'lsf' would caused all jobs
      to be submitted with bsub -n 3.  Returns None if no native_specification is required.
    """

    cpu_req = task.cpu_req
    mem_req = task.mem_req
    time_req = task.time_req

    # return '-l h_vmem={mem_req}M,num_proc={cpu_req}'.format(
    return '-l cpu={cpu_req} -q {q}'.format(mem_req=mem_req,
                                            cpu_req=cpu_req,
                                            q='dev-short')

if __name__ == '__main__':
    kosmos_app = Kosmos('sqlite.db', get_submit_args=get_submit_args)
    kosmos_app.initdb()

    r = Recipe()

    echo = r.add_source([tools.Echo(tags={'word': 'hello'}), tools.Echo(tags={'word': 'world'})])
    cat = r.add_stage(tools.Cat, parents=[echo], rel=rel.One2many([('n', [1, 2])]))



    ex = Execution.start(kosmos_app, 'Simple', 'out/simple2', max_attempts=2, drm='ge', restart=True, skip_confirm=True)
    ex.run(r)