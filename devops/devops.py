import os
import subprocess
from shutil import rmtree

from cosmos import __version__
import argparse


def run(cmd):
    print(f"* {cmd}")
    subprocess.run(cmd, shell=True, executable="/bin/bash", check=True)


def release():
    """
    run inside a conda environment with conda-build installed
    """

    run("python setup.py sdist upload")

    if os.path.exists("../cosmos-wfm"):
        print("removing cosmos-wfm dir")
        rmtree("../cosmos-wfm")

    run(f"conda skeleton pypi cosmos-wfm --version {__version__}")
    run(f"conda build cosmos-wfm")

    # conda base dir, ex: /home/egafni/miniconda3
    conda_base = subprocess.check_output("conda info --base", shell=True)

    run(
        f"anaconda upload {conda_base}/conda-bld/linux-64/cosmos-wfm-{__version__}-py38_0.tar.bz2 -u ravelbio"
    )


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("cmd")
    args = p.parse_args()
    globals()[args.cmd]()
