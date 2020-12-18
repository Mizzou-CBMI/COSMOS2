import glob
import subprocess as sp

import pytest

from cosmos.constants import REPO_DIR


def run(cmd):
    return sp.check_call(cmd, shell=True)


@pytest.mark.parametrize("script_path", glob.glob(f"{REPO_DIR}/examples/*.py"))
def test_examples(cleandir, script_path):
    run(f"python {script_path}")
