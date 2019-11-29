import os

from cosmos.api import Cosmos, py_call
from cosmos.util.helpers import environment_variables, temp_cwd


def use_cuda_device(some_arg, num_gpus):
    assert 'CUDA_VISIBLE_DEVICES' in os.environ
    print('some_arg', some_arg)
    print('CUDA_VISIBLE_DEVICES', os.environ['CUDA_VISIBLE_DEVICES'])
    assert len(os.environ['CUDA_VISIBLE_DEVICES'].split(',')) == num_gpus


def main():
    cosmos = Cosmos()
    cosmos.initdb()
    workflow = cosmos.start('test', skip_confirm=True)
    for i, num_gpus in enumerate([1, 1, 2, 2, 3]):
        task = workflow.add_task(use_cuda_device,
                                 dict(some_arg=i, num_gpus=num_gpus),
                                 gpu_req=num_gpus,
                                 uid=str(i))

    workflow.run(max_gpus=len(os.environ['COSMOS_LOCAL_GPU_DEVICES'].split(',')),
                 cmd_wrapper=py_call, do_cleanup_atexit=False)


def test_main():
    with temp_cwd():
        with environment_variables(COSMOS_LOCAL_GPU_DEVICES='0,1,3'):
            main()


if __name__ == '__main__':
    with environment_variables(COSMOS_LOCAL_GPU_DEVICES='0,1,3'):
        main()
