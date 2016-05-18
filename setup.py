from distutils.core import setup
import os
import re
import sys

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


install_requires = [
    "decorator",
    "flask",
    'funcsigs',
    'blinker',
    "sqlalchemy",
    "black_magic==0.0.10",  # to get a signature preserving partial() in cosmos.api
    'flask-sqlalchemy',
    'networkx',
    "enum34",
    "six",
    "SQLAlchemy-Utils",
    # "pyparsing==1.5.7",
    'psutil',
    "drmaa",
    'more_itertools'
]

if sys.version_info < (3,):
    # package_dir = {'': 'cosmos'}
    package_data = {'cosmos': list(find_all('cosmos/', '.py|.pyc$', inverse=True, remove_prefix=True))}
    install_requires += ['futures', 'configparser']
else:
    package_dir = {'': 'cosmos'}
    package_data = {'cosmos': list(find_all('cosmos/', '.py|.pyc$', inverse=True, remove_prefix=True))}

setup(
    name="cosmos-wfm",
    version=__version__,
    scripts=['bin/cosmos'],
    description="Workflow Management System",
    url="https://cosmos.hms.harvard.edu/",
    author="Erik Gafni",
    author_email="egafni@gmail.com",
    maintainer="Erik Gafni",
    maintainer_email="egafni@gmail.com",
    license="MIT",
    install_requires=install_requires,
    setup_requires=['pytest-runner'],
    tests_require=['pytest'],
    packages=find_packages(),
    # package_dir=package_dir,
    include_package_data=True,
    package_data=package_data,
    classifiers=[
        'Programming Language :: Python :: 2.7',
        # 'Programming Language :: Python :: 3',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: MacOS',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: POSIX',
        'Programming Language :: Python',
        'Topic :: Software Development',
        'Topic :: Utilities',
    ]
)
