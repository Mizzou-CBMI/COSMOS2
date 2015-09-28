from distutils.core import setup
import os
import re

from setuptools import find_packages


with open(os.path.join(os.path.dirname(__file__), 'cosmos/VERSION'), 'r') as fh:
    __version__ = fh.read().strip()


def find_all(path, reg_expr, inverse=False, remove_prefix=False):
    if not path.endswith('/'):
        path = path + '/'
    for root, dirnames, filenames in os.walk(path):
        for filename in filenames:
            match = re.search(reg_expr, filename) is not None
            if inverse:
                match = not match
            if match:
                out = os.path.join(root, filename)
                if remove_prefix:
                    out = out.replace(path, '')
                yield out

setup(
    name="cosmos-wfm",
    version=__version__,
    description="Workflow Management System",
    url="https://cosmos.hms.harvard.edu/",
    author="Erik Gafni",
    author_email="egafni@gmail.com",
    maintainer="Erik Gafni",
    maintainer_email="egafni@gmail.com",
    license="GPLv3",
    install_requires=[
        'gntp',
        "psprofile",
        "decorator",
        'futures',
        "Flask",
        'blinker',
        "sqlalchemy",
        "black_magic==0.0.10", # to get a signature preserving partial() in cosmos.api
        'Flask-Admin',
        'flask-sqlalchemy',
        'funcsigs',
        'flask_sqlalchemy_session',
        'networkx',
        'configparser',
        "enum34",
        "Flask-Failsafe",
        "six",
        "SQLAlchemy-Utils",
        "pyparsing==1.5.7",
        "recordtype"
    ],
    packages=find_packages(),
    include_package_data=True,
    package_data={'cosmos': list(find_all('cosmos/', '.py|.pyc$', inverse=True, remove_prefix=True))}
)
