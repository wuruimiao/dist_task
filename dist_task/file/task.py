
from pathlib import Path
from dist_task.abstract.task import Task
from dist_task.utils.errno import Error, OK


class FileTask(Task):
    def __init__(self, status_dir: Path, name, task_dir: Path):
        self._id = name
        self._todo = status_dir.joinpath(f"{self._id}.todo")
        self._ing = status_dir.joinpath(f"{self._id}.ing")
        self._success = status_dir.joinpath(f"{self._id}.success")
        self._fail = status_dir.joinpath(f"{self._id}.fail")
        self._task_dir = task_dir

    @property
    def task_dir(self) -> Path:
        return self._task_dir.joinpath(self.id)

    @property
    def id(self) -> str:
        return self._id

    def is_todo(self) -> bool:
        return self._todo.exists()

    def is_ing(self) -> bool:
        return self._ing.exists()

    def mark_todo(self) -> Error:
        self._todo.touch()
        return OK

    def mark_ing(self) -> Error:
        self._todo.unlink()
        self._ing.touch()
        return OK

    def mark_success(self) -> Error:
        self._ing.unlink()
        self._success.touch()
        return OK

    def mark_fail(self) -> Error:
        self._ing.unlink()
        self._success.touch()
        return OK
