from distutils.core import setup
from setuptools import find_packages

setup(
    # Metadata
    name="cosmos-wfm",
    version="0.5",
    description="Workflow Management System",
    url="https://cosmos.hms.harvard.edu/",
    author="Erik Gafni",
    author_email="egafni@gmail.com",
    maintainer="Erik Gafni",
    maintainer_email="egafni@gmail.com",
    license="Non-commercial use only",
    install_requires=[
        "psutil",
        'recordtype',
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
    package_data={'': ['examples/*']}

)


