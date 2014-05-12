from setuptools import find_packages, setup

setup(
    # Metadata
    name="kosmos",
    version="0.5",
    description="Workflow Management System",
    url="",
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
        #'Flask-DebugToolbar',
        'Flask-SQLAlchemy',
        'Flask-WTF',
        'networkx',
        # "pyzmq",
        # "ipdb",
        # "ipython",
        "enum34"],
    scripts=["bin/kosmos"],
    # Packaging Instructions
    packages=find_packages(),
    include_package_data=True
)


