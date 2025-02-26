import math
from collections import OrderedDict
from pathlib import Path
from typing import Optional

from common_tool.errno import Error, OK
from common_tool.log import logger
from common_tool.system import sync_files

from dist_task.abstract import Worker
from dist_task.abstract.task import Task
from dist_task.file.common import list_dir
from dist_task.file.config import USER, is_remote
from dist_task.file.task import FileTask

SYNC = Error(5555, 'sync fail', 'sync同步失败')
NO_FILE = Error(6666, 'no file', 'sync同步没有文件')


class FileStorage:
    def __init__(self, status_dir: str, task_dir: str, pull: bool):
        self.status_dir = Path(status_dir)
        self.task_dir = Path(task_dir)
        self.pull = pull


class FileWorker(Worker):

    def __init__(self, host: str, con: int, free: int, storages: [FileStorage]):
        self._storages = storages
        self._host = host
        self.set_con(con)
        self.set_free(free)

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

    def _make_task(self, task_id: str, storage: FileStorage) -> FileTask:
        return FileTask(task_id, storage.task_dir, storage.status_dir, self._host)

    def get_the_task(self, task_id) -> Optional[FileTask]:
        for storage in self._storages:
            task = self._make_task(task_id, storage)
            if not task.is_init():
                return task
        return None

    def _get_storage_unfinished_id(self) -> dict[FileStorage, list[str]]:
        return OrderedDict([
            (storage, [task.id() for task in tasks if task.is_ing() or task.is_todo()])
            for storage, tasks in self.get_all_tasks().items()
        ])

    def do_push_task(self, task_id: str, task_storage: Path) -> tuple[FileTask, Error]:
        storage_unfinished = self._get_storage_unfinished_id()
        storage = min(storage_unfinished, key=lambda key: len(storage_unfinished[key]))

        logger.info(f'start upload {task_storage} to {self.id()}')
        ok = self._sync(str(task_storage), str(storage.task_dir))
        logger.info(f'end upload {task_storage} to {self.id()} {ok}')

        task = self._make_task(task_id, storage)
        if not ok:
            return task, SYNC
        return task, OK

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
        task_ids = []
        [task_ids.extend(ids) for ids in self._get_storage_unfinished_id().values()]
        return task_ids

    def get_all_tasks(self) -> dict[FileStorage, list[FileTask]]:
        tasks = OrderedDict()
        for storage in self._storages:
            files, _ = list_dir(self.is_remote(), self._host, USER, storage.status_dir)
            tasks[storage] = [self._make_task(task_id, storage) for task_id in files.keys()]
        return tasks

    def get_ing_tasks(self) -> [Task]:
        return [task for tasks in self.get_all_tasks().values() for task in tasks if task.is_ing()]

    @staticmethod
    def _balanced_selection(arr: [[]], limit: int):
        total_elements = sum(len(sublist) for sublist in arr)
        result = []

        if total_elements <= limit:
            return [item for sublist in arr for item in sublist]

        remaining_limit = limit
        for sublist in arr:
            # 计算每个子数组应该取的数量，尽量平均分配
            count = min(len(sublist), math.ceil(len(sublist) / total_elements * limit))
            selected = sublist[:count]  # 从子数组取前 count 个元素
            result.extend(selected)
            remaining_limit -= len(selected)

            if remaining_limit <= 0:
                break
        return result

    def get_todo_tasks(self, limit: int) -> [Task]:
        if limit <= 0:
            return []
        all_todos = [[task for task in tasks if task.is_todo()] for tasks in self.get_all_tasks().values()]
        all_todos = [tasks for tasks in all_todos if len(tasks) > 0]
        return self._balanced_selection(all_todos, limit)

    def get_done_tasks(self):
        return [task for tasks in self.get_all_tasks().values() for task in tasks if task.is_done()]

    def get_to_pull_tasks(self) -> [Task]:
        return [task for storage, tasks in self.get_all_tasks().items() for task in tasks
                if task.is_success() and storage.pull]
