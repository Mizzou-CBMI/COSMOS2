import os
import subprocess as sp
import sys

from cosmos.api import Cosmos

image = '638253504273.dkr.ecr.us-west-1.amazonaws.com/ravel:bba83ce2c116f097f97a1a6255df6d80bbb5e878'


def say(text, out_file):
    return r"""
        echo "{text}" > {out_file}
    """.format(text=text, out_file=out_file)


if __name__ == '__main__':
    cosmos = Cosmos('sqlite:///%s/sqlite.db' % os.path.dirname(os.path.abspath(__file__)),
                    default_drm='awsbatch',
                    default_drm_options=dict(container_image=image,
                                             s3_bucket_for_temp_files='ravel-cosmos'),
                    default_queue='pipe')
    cosmos.initdb()

    sp.check_call('mkdir -p analysis_output/ex1', shell=True)
    os.chdir('analysis_output/ex1')
    workflow = cosmos.start('Example1', restart=True, skip_confirm=True)

    t = workflow.add_task(func=say,
                          params=dict(text='Hello World', out_file='out.txt'),
                          uid='my_task', time_req=None, core_req=1, mem_req=1024)

    print('task.params', t.params)
    print('task.input_map', t.input_map)
    print('task.output_map', t.output_map)
    print('task.core_req', t.core_req)
    print('task.time_req', t.time_req)
    print('task.drm', t.drm)
    print('task.uid', t.uid)
    print('task.drm_options', t.drm_options)
    print('task.queue', t.queue)
    workflow.run()

    sys.exit(0 if workflow.successful else 1)
