from distutils.core import setup
import os
import re

from setuptools import find_packages


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
    # Metadata
    name="cosmos-wfm",
    version="0.10.0",
    description="Workflow Management System",
    url="https://cosmos.hms.harvard.edu/",
    author="Erik Gafni",
    author_email="egafni@gmail.com",
    maintainer="Erik Gafni",
    maintainer_email="egafni@gmail.com",
    license="Non-commercial use only",
    install_requires=[
        "psutil",
        "flask",
        'blinker',
        'Flask-Admin',
        'Flask-SQLAlchemy',
        'Flask-WTF',
        'networkx',
        "ipython",
        "enum34"],
    # Packaging Instructions
    packages=find_packages(),
    include_package_data=True,
    package_data={'cosmos': list(find_all('cosmos/', '.py|.pyc$', True, True))}
)


