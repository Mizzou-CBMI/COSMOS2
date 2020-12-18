import pytest

from cosmos.api import Cosmos, py_call
from cosmos.models.Workflow import DuplicateUid, InvalidParams

environment_variables_dict = {"Cosmos": "jobs", "are": "very", "cool": "!"}


def prepare_data(a):
    print(a)


def train_machine_learning_model(a):
    print(a)


def main():
    cosmos = Cosmos().initdb()
    workflow = cosmos.start("duplicate_uids", skip_confirm=True)
    task = workflow.add_task(func=prepare_data, params=dict(a=1), uid="x")

    # normally you can't add a task with the same uid to the same stage
    with pytest.raises(DuplicateUid):
        workflow.add_task(func=prepare_data, params=dict(a=1), uid="x")

    # normally you can't add a task with the same uid to the same stage
    with pytest.raises(DuplicateUid):
        workflow.add_task(func=prepare_data, params=dict(a=1), uid="x")

    # set if_duplicate="return" to True to get the same task back that you added
    task2 = workflow.add_task(func=prepare_data, params=dict(a=1), uid="x", if_duplicate="return")
    assert task == task2

    # this can be especially useful in loops to avoid repeating computation
    for _ in range(3):
        task = workflow.add_task(func=prepare_data, params=dict(a=1), uid="x", if_duplicate="return")
        workflow.add_task(
            func=train_machine_learning_model, params=dict(a=1), uid="x", if_duplicate="return", parents=task
        )

    # NOTE: parameters must be identical when using this feature
    with pytest.raises(InvalidParams):
        workflow.add_task(func=prepare_data, params=dict(a=1000), uid="x", if_duplicate="return")


if __name__ == "__main__":
    main()
