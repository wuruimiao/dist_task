from pathlib import Path

from common_tool.errno import Error, OK
from common_tool.log import logger
from common_tool.system import sync_files

from dist_task.abstract import Worker
from dist_task.abstract.task import Task, TaskStatus
from dist_task.file.common import list_dir
from dist_task.file.config import USER, is_remote
from dist_task.file.task import FileTask

SYNC = Error(5555, 'sync fail', 'sync同步失败')
NO_FILE = Error(6666, 'no file', 'sync同步没有文件')


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

    def _sync(self, _from, _to, to_remote: bool = True) -> bool:
        if self.is_remote():
            _, ok = sync_files(_from, _to, to_remote, self._host, USER)
        else:
            _, ok = sync_files(_from, _to)
        return ok

    def id(self) -> str:
        return self._host

    def upload_task(self, task_dir: Path) -> Error:
        """
        :param task_dir: proxy的任务路径
        :return:
        """
        logger.info(f'start upload {task_dir} to {self.id()}')
        ok = self._sync(str(task_dir), str(self._task_dir))
        logger.info(f'end upload {task_dir} to {self.id()} {ok}')
        if not ok:
            return SYNC
        return OK

    def get_the_task(self, task_id) -> FileTask:
        return FileTask(task_id, self._task_dir, self._status_dir, self._host)

    def do_push_task(self, task: FileTask) -> Error:
        return OK

    def do_pull_task(self, task: FileTask, local_dir) -> Error:
        task_dir, _ = task.task_dir()
        if not task_dir:
            return NO_FILE
        logger.info(f'start pull {task_dir} from {self.id()}')
        ok = self._sync(str(task_dir), local_dir, to_remote=False)
        logger.info(f'end pull {task_dir} from {self.id()}')
        if ok:
            return OK
        return SYNC

    def get_unfinished_id(self) -> [str]:
        return [task.id() for task in self._get_all_tasks() if task.is_ing() or task.is_todo()]

    def _get_all_tasks(self):
        files, _ = list_dir(self.is_remote(), self._host, USER, self._status_dir)
        tasks = [FileTask(name, self._task_dir, self._status_dir, self._host) for name in files.keys()]
        return tasks

    def get_ing_tasks(self) -> [Task]:
        return [task for task in self._get_all_tasks() if task.is_ing()]

    def get_todo_tasks(self) -> [Task]:
        return [task for task in self._get_all_tasks() if task.is_todo()]

    def get_done_tasks(self):
        return [task for task in self._get_all_tasks() if task.is_done()]
