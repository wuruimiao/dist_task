import math
from collections import OrderedDict
from pathlib import Path
from typing import Optional, Callable, Any, List, Tuple

from common_tool.errno import Error, OK
from common_tool.log import logger
from common_tool.system import sync_files

from dist_task.abstract import Worker
from dist_task.abstract.task import Task, TaskT
from dist_task.file.common import list_dir
from dist_task.file.config import USER, is_remote
from dist_task.file.task import FileTask

SYNC = Error(5555, 'sync fail', 'sync同步失败')
NO_FILE = Error(6666, 'no file', 'sync同步没有文件')


class FileStorage:
    def __init__(self, status_dir: str, task_dir: str, pull: bool):
        self._status_dir = Path(status_dir)
        self._task_dir = Path(task_dir)
        self._pull = pull

    @property
    def status_dir(self) -> Path:
        return self._status_dir

    @property
    def task_dir(self) -> Path:
        return self._task_dir

    @property
    def pull(self) -> bool:
        return self._pull

    def __str__(self):
        return f'status={self.status_dir} task={self.task_dir} pull={self.pull}'

    def __repr__(self):
        return str(self)


class FileWorker(Worker):
    def __init__(self, host: str, con: int, storages: [FileStorage],
                 handlers: List[Tuple[Callable[[TaskT, Any], Error], Any]],
                 free: int = None, auto_clean: bool = False, ):
        super().__init__(handlers=handlers, con=con, free=free, auto_clean=auto_clean)
        self.storages = tuple(storages)
        self.host = host

    def is_remote(self) -> bool:
        return is_remote(self.host)

    def _sync(self, _from, _to, is_push: bool = True) -> bool:
        logger.info(f'sync from {_from} to {_to} is_remote={self.is_remote()} is_push={is_push}')
        if self.is_remote():
            _, ok = sync_files(_from, _to, is_push, self.host, USER)
        else:
            _, ok = sync_files(_from, _to)
        return ok

    def id(self) -> str:
        return self.host

    def info(self) -> str:
        return f'id={self.id()} con={self.concurrency} storages={self.storages} ' \
               f'handler_num={len(self.handlers)} free={self.free} auto_clean={self.auto_clean}'

    def _make_task(self, task_id: str, storage: FileStorage) -> FileTask:
        return FileTask(task_id, storage.task_dir, storage.status_dir, self.host)

    def get_the_task(self, task_id: str) -> Optional[FileTask]:
        for storage in self.storages:
            task = self._make_task(task_id, storage)
            if not task.is_init():
                return task
        return None

    def _get_storage_unfinished_id(self) -> dict[FileStorage, list[str]]:
        return OrderedDict([
            (storage, [task.id() for task in tasks if task.is_ing() or task.is_todo()])
            for storage, tasks in self.get_all_tasks().items()
        ])

    def do_push_task(self, task_id: str, from_storage: Path) -> tuple[FileTask, Error]:
        storage_unfinished = self._get_storage_unfinished_id()
        storage = min(storage_unfinished, key=lambda key: len(storage_unfinished[key]))
        ok = self._sync(str(from_storage), str(storage.task_dir))

        task = self._make_task(task_id, storage)
        if not ok:
            return task, SYNC
        return task, OK

    def do_pull_task(self, task: FileTask, to_storage: Path) -> Error:
        task_dir, _ = task.task_dir()
        if not task_dir:
            return NO_FILE
        logger.info(f'start pull {task_dir} from {self.id()}')
        ok = self._sync(str(task_dir), to_storage, is_push=False)
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
        for storage in self.storages:
            files, _ = list_dir(self.is_remote(), self.host, USER, storage.status_dir)
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
