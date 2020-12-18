import glob
import subprocess as sp

import pytest

from cosmos.api import cd
from cosmos.constants import REPO_DIR


def run(cmd):
    return sp.check_call(cmd, shell=True)


EXAMPLES = glob.glob(f"{REPO_DIR}/examples/*.py")
EXAMPLES = [e for e in EXAMPLES if "awsbatch" not in e]


@pytest.mark.parametrize("script_path", EXAMPLES)
def test_examples(cleandir, script_path):
    with cd(cleandir):
        run(f"python {script_path}")
