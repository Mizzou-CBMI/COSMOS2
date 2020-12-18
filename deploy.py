import subprocess as sp
import os


def main(new_version):
    def run(cmd):
        os.system(cmd)

    run("python setup.py sdist upload")
    run("curl http://readthedocs.org/build/cosmos-wfm")


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser()
    # p.add_argument('new_version')
    args = p.parse_args()

    main(**vars(args))
