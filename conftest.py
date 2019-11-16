import os
import shutil
import tempfile

import pytest


@pytest.fixture()
def cleandir():
    oldpath = os.getcwd()
    newpath = tempfile.mkdtemp()
    os.chdir(newpath)
    yield newpath

    os.chdir(oldpath)
    shutil.rmtree(newpath)
