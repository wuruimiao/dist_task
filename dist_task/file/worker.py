from pathlib import Path

from common_tool.errno import Error, OK
from common_tool.log import logger
from common_tool.system import sync_files

from dist_task.abstract import Worker
from dist_task.abstract.task import Task, TaskStatus
from dist_task.file.common import list_dir
from dist_task.file.config import USER, is_remote
from dist_task.file.task import FileTask


class FileWorker(Worker):
    def __init__(self, status_dir: str, task_dir: str, host: str, con: int):
        self._status_dir = Path(status_dir)
        self._task_dir = Path(task_dir)
        self._host = host
        self.set_con(con)

    def set_local(self):
        self._host = 'local'

    def is_remote(self) -> bool:
        return is_remote(self._host)

    def _sync(self, _from, _to, to_remote: bool = True):
        if self.is_remote():
            sync_files(_from, _to, to_remote, self._host, USER)
        else:
            sync_files(_from, _to)

    @property
    def id(self) -> str:
        return self._host

    def upload_task(self, task_dir: Path) -> Error:
        """
        :param task_dir: proxy的任务路径
        :return:
        """
        logger.info(f'start upload {task_dir} to {self.id}')
        self._sync(str(task_dir), str(self._task_dir))
        logger.info(f'end upload {task_dir} to {self.id}')
        return OK

    def push_task(self, task_id) -> Error:
        task = FileTask(task_id, self._task_dir, self._status_dir, self._host)
        return task.todo()

    def pull_task(self, task_id, local_dir: str) -> Error:
        task = FileTask(task_id, self._task_dir, self._status_dir, self._host)
        task_dir, _ = task.task_dir()
        logger.info(f'start pull {task_dir} from {self.id}')
        self._sync(str(task_dir), local_dir, to_remote=False)
        logger.info(f'end pull {task_dir} from {self.id}')
        task.done()
        return OK

    def get_task_status(self, task_id: str) -> [TaskStatus, Error]:
        task = FileTask(task_id, self._task_dir, self._status_dir, self._host)
        return task.status(), OK

    def get_unfinished_id(self) -> [str]:
        return [task.id for task in self.get_all_tasks() if task.is_ing() or task.is_todo()]

    def get_all_tasks(self):
        files, _ = list_dir(self.is_remote(), self._host, USER, self._status_dir)
        tasks = [FileTask(name, self._task_dir, self._status_dir, self._host) for name in files.keys()]
        return tasks

    def get_ing_tasks(self) -> [Task]:
        return [task for task in self.get_all_tasks() if task.is_ing()]

    def get_todo_tasks(self) -> [Task]:
        return [task for task in self.get_all_tasks() if task.is_todo()]

    def get_done_tasks(self):
        return [task for task in self.get_all_tasks() if task.is_done()]
