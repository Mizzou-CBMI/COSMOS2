from setuptools import setup, find_packages
import os
import re
import sys

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
    "flask",
    'funcsigs',
    'blinker',
    "sqlalchemy",
    'networkx>=2.0',
    "six",
    "drmaa",
    'more-itertools',
    "decorator",
    "python-dateutil",
]
package_data = {'cosmos': list(find_all('cosmos/', '.py|.pyc$', inverse=True, remove_prefix=True))}

if sys.version_info < (3,):
    install_requires += ['subprocess32']
if sys.version_info < (3, 6):
    install_requires += ['enum34']

setup(
    name="cosmos-wfm",
    version=__version__,
    scripts=['bin/cosmos','bin/run_pyfunc'],
    description="Workflow Management System",
    long_description='Cosmos is a library for writing analysis pipelines, and is particularly suited pipelines which analyze next generation sequencing genomic'
                     'data. See https://github.com/Mizzou-CBMI/COSMOS2 for details.',
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
    include_package_data=True,
    package_data=package_data,
    # package_dir = {'cosmos': 'cosmos'},
    classifiers=[
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: MacOS',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: POSIX',
        'Programming Language :: Python',
        'Topic :: Software Development',
        'Topic :: Utilities',
    ],
    use_2to3=True,
    use_2to3_exclude_fixers=['lib2to3.fixes.fix_import'],
    keywords='workflow pipeline ngs manager management distributed sge slurm genomics sequencing grid computing scientific',
)
