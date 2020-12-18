import os
from functools import partial

from cosmos.api import Cosmos, py_call_cmd_wrapper
from cosmos.models.Workflow import default_task_log_output_dir
from cosmos.util.helpers import environment_variables


def use_cuda_device(some_arg, num_gpus):
    assert "CUDA_VISIBLE_DEVICES" in os.environ
    print(("some_arg", some_arg))
    print(("CUDA_VISIBLE_DEVICES", os.environ["CUDA_VISIBLE_DEVICES"]))
    assert len(os.environ["CUDA_VISIBLE_DEVICES"].split(",")) == num_gpus


def main(output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    cosmos = Cosmos()
    cosmos.initdb()
    workflow = cosmos.start(
        "gpu", skip_confirm=True, primary_log_path=os.path.join(output_dir, "workflow.log"),
    )
    for i, num_gpus in enumerate([1, 1, 2, 2, 3]):
        task = workflow.add_task(
            use_cuda_device, dict(some_arg=i, num_gpus=num_gpus), gpu_req=num_gpus, uid=str(i),
        )

    workflow.run(
        max_gpus=len(os.environ["COSMOS_LOCAL_GPU_DEVICES"].split(",")),
        cmd_wrapper=py_call_cmd_wrapper,
        do_cleanup_atexit=False,
        log_out_dir_func=partial(default_task_log_output_dir, prefix="%s" % output_dir),
    )


if __name__ == "__main__":
    with environment_variables(COSMOS_LOCAL_GPU_DEVICES="0,1,3"):
        main(output_dir="analysis_output/ex_gpu")
