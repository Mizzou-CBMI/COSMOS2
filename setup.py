import os
import re
import sys

from setuptools import setup, find_packages

with open(os.path.join(os.path.dirname(__file__), "cosmos/VERSION"), "r") as fh:
    __version__ = fh.read().strip()


def find_all(path, reg_expr, inverse=False, remove_prefix=False):
    if not path.endswith("/"):
        path = path + "/"
    for root, dirnames, filenames in os.walk(path):
        for filename in filenames:
            match = re.search(reg_expr, filename) is not None
            if inverse:
                match = not match
            if match:
                out = os.path.join(root, filename)
                if remove_prefix:
                    out = out.replace(path, "")
                yield out


install_requires = [
    "funcsigs",
    "boto3",
    "blinker",
    "sqlalchemy",
    "networkx>=2.0",
    "six",
    "drmaa",
    "more-itertools",
    "decorator",
    "python-dateutil",
]
package_data = {"cosmos": list(find_all("cosmos/", ".py|.pyc$", inverse=True, remove_prefix=True))}


setup(
    name="cosmos-wfm",
    version=__version__,
    scripts=["bin/cosmos", "bin/run_pyfunc"],
    description="Workflow Management System",
    long_description="Cosmos is a library for writing analysis pipelines, and is particularly suited pipelines "
    "which analyze next generation sequencing genomic"
    "data. See https://github.com/Mizzou-CBMI/COSMOS2 for details.",
    url="https://mizzou-cbmi.github.io/",
    author="Erik Gafni",
    author_email="egafni@gmail.com",
    maintainer="Erik Gafni",
    maintainer_email="egafni@gmail.com",
    license="GPL v3",
    install_requires=install_requires,
    extras_require={"web": ["flask"], "test": ["flask", "ipython", "sphinx_rtd_theme", "black"]},
    packages=find_packages(),
    include_package_data=True,
    package_data=package_data,
    # package_dir = {'cosmos': 'cosmos'},
    classifiers=[
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: MacOS",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: POSIX",
        "Programming Language :: Python",
        "Topic :: Software Development",
        "Topic :: Utilities",
    ],
    keywords="workflow machine learning ipeline ngs manager management distributed sge "
    "slurm genomics sequencing grid computing scientific",
)
