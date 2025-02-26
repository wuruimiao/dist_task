import functools
import math
import time
from abc import ABCMeta, abstractmethod
from multiprocessing import Pool, Value
from typing import Any, Optional

from common_tool.errno import Error
from common_tool.log import logger

from dist_task.abstract.task import Task, TaskStatus, INIT


class Worker(metaclass=ABCMeta):
    _handlers = []
    _concurrency = 1
    _free = 2

    @abstractmethod
    def id(self) -> str:
        pass

    @abstractmethod
    def is_remote(self) -> bool:
        pass

    @classmethod
    def set_handler(cls, func, position=0):
        cls._handlers.insert(position, func)

    def set_con(self, num: int):
        self._concurrency = num

    def set_free(self, num: int):
        self._free = num

    @abstractmethod
    def get_the_task(self, task_id) -> Optional[Task]:
        pass

    @abstractmethod
    def do_pull_task(self, task: Task, storage: Any) -> Error:
        pass

    @abstractmethod
    def do_push_task(self, task_id: id, task_storage: Any) -> tuple[Task, Error]:
        pass

    def pull_task(self, task: Task, storage: Any) -> Error:
        err = self.do_pull_task(task, storage)
        if not err.ok:
            return err
        return task.done()

    def push_task(self, task_id: str, task_storage: Any) -> Error:
        task, err = self.do_push_task(task_id, task_storage)
        if not err.ok:
            return err
        return task.todo()

    def get_task_status(self, task_id: str) -> TaskStatus:
        task = self.get_the_task(task_id)
        if not task:
            return INIT
        return task.status()

    @abstractmethod
    def get_unfinished_id(self) -> [str]:
        pass

    def free_num(self) -> int:
        return max(0, math.ceil(self._free - len(self.get_unfinished_id())))

    @abstractmethod
    def get_ing_tasks(self) -> [Task]:
        pass

    @abstractmethod
    def get_todo_tasks(self, limit: int) -> [Task]:
        pass

    @abstractmethod
    def get_done_tasks(self) -> [Task]:
        pass

    @abstractmethod
    def get_to_pull_tasks(self) -> [Task]:
        pass

    def _do(self, task: Task) -> Error:
        err: Error
        err = task.ing()
        if not err.ok:
            return err

        for handle in self._handlers:
            err = handle(task)
            if not err.ok:
                task.fail()
                return err
        return task.success()

    def handle_task(self, task: Task, ing_num: Value) -> Error:
        logger.info(f"start handle task {task.id()} {len(self._handlers)} {self._concurrency}")
        with ing_num.get_lock():
            ing_num.value += 1
        err = self._do(task)
        with ing_num.get_lock():
            ing_num.value += 1
        logger.info(f'end handle task {task.id()} {err}')
        return err

    def start(self, auto_clean=False):
        ing_num = Value('i', 0)
        logger.info(f'start with {self._concurrency} concurrent')
        with Pool(processes=self._concurrency) as pool:
            for task in self.get_ing_tasks():
                task.todo(force=True)

            while True:
                limit = self._concurrency - ing_num.value
                if limit > 0:
                    logger.info(f'start get {limit} todo and handle')
                todos = self.get_todo_tasks(limit)
                if todos:
                    logger.info(f'start push {len(todos)} to handle')
                    for task in todos:
                        pool.apply_async(self.handle_task, args=(task, ing_num))

                if auto_clean:
                    for task in self.get_done_tasks():
                        task.clean()

                time.sleep(1)


def handler(position=0):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            Worker.set_handler(func, position)
            return func(*args, **kwargs)

        return wrapper

    return decorator
