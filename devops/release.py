#!/usr/bin/env python
import os
import subprocess
from argparse import ArgumentParser
from shutil import rmtree

from cosmos import __version__


def run(cmd):
    print(f"* {cmd}")
    subprocess.run(cmd, shell=True, executable="/bin/bash", check=True)


def main(args):
    """
    Must be run inside a conda environment with conda-build installed

    conda create -n cosmos
    conda activate cosmos
    conda install conda-build ghp-import sphinx sphinx_rtd_theme python>3
    python setup.py develop

    AFTER cosmos/VERSION is bumped and committed you can run:
    $ devops/release.py
    """

    if not args.skip_pypi:
        run("python setup.py sdist upload")

    if not args.skip_conda:
        if os.path.exists("cosmos-wfm"):
            print("removing cosmos-wfm dir")
            rmtree("cosmos-wfm")

        run(f"conda skeleton pypi cosmos-wfm --version {__version__}")
        run(f"conda build cosmos-wfm")

        # conda base dir, ex: /home/egafni/miniconda3
        conda_base = subprocess.check_output("conda info --base", shell=True)

        run(
            f"anaconda upload {conda_base}/conda-bld/linux-64/cosmos-wfm-{__version__}-py38_0.tar.bz2 -u ravelbio"
        )

    if not args.skip_docs:
        # requires packages: ghp-import sphinx sphinx_rtd_theme
        run(
            f"""
                cd docs
                make html
                cd build/html
                ghp-import -n ./ -p        
            """
        )


if __name__ == "__main__":
    p = ArgumentParser()
    p.add_argument("--skip-pypi", action="store_true")
    p.add_argument("--skip-conda", action="store_true")
    p.add_argument("--skip-docs", action="store_true")
    args = p.parse_args()
    main(args)
