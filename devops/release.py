import os
import subprocess
from shutil import rmtree

from cosmos import __version__
import argparse


def run(cmd):
    print(f"* {cmd}")
    subprocess.run(cmd, shell=True, executable="/bin/bash", check=True)


def main():
    """
    run inside a conda environment with conda-build installed

    AFTER cosmos/VERSION is bumped
    """

    run("python setup.py sdist upload")

    # run("curl http://readthedocs.org/build/cosmos-wfm")

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

    # reqiures pip install ghp-import sphinx sphinx_rtd_theme
    run(
        f"""
            cd docs
            make html
            cd build/html
            ghp-import -n ./ -p        
        """
    )


if __name__ == "__main__":
    main()
