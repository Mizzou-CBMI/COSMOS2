import shutil
import tempfile

from cosmos.api import Cosmos, cd, py_call


environment_variables_dict = {"Cosmos": "jobs", "are": "very", "cool": "!"}


def command_with_env_variables():
    lines = ["import os"] + [
        f"assert os.getenv({k}) == {v}"
        for k, v in environment_variables_dict.items()  # this makes assert variable == value for each env variable
    ]
    command = f"python -c \"{';'.join(lines)}\""
    return command


def main():
    cosmos = Cosmos()
    cosmos.initdb()
    workflow = cosmos.start("workflow", skip_confirm=True)
    workflow.add_task(
        func=command_with_env_variables, environment_variables=environment_variables_dict, uid="special"
    )
    workflow.run(cmd_wrapper=py_call)


if __name__ == "__main__":
    main()
