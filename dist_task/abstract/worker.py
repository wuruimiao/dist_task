import functools
import math
import time
from abc import ABCMeta, abstractmethod
from multiprocessing import Pool
from typing import Any

from common_tool.errno import Error
from common_tool.log import logger

from dist_task.abstract.task import Task, TaskStatus


class Worker(metaclass=ABCMeta):
    _handlers = []
    _concurrency = 1

    @abstractmethod
    def id(self) -> str:
        pass

    @abstractmethod
    def is_remote(self) -> bool:
        pass

    @classmethod
    def set_handler(cls, func, position=0):
        cls._handlers.insert(position, func)

    def set_con(self, num=1):
        self._concurrency = num

    @abstractmethod
    def get_the_task(self, task_id) -> Task:
        pass

    @abstractmethod
    def do_pull_task(self, task: Task, storage: Any) -> Error:
        pass

    @abstractmethod
    def do_push_task(self, task: Task) -> Error:
        pass

    def pull_task(self, task: Task, storage: Any) -> Error:
        err = self.do_pull_task(task, storage)
        if not err.ok:
            return err
        return task.done()

    def push_task(self, task: Task) -> Error:
        err = self.do_push_task(task)
        if not err.ok:
            return err
        return task.todo()

    def get_task_status(self, task_id: str) -> TaskStatus:
        task = self.get_the_task(task_id)
        return task.status()

    @abstractmethod
    def get_unfinished_id(self) -> [str]:
        pass

    def free_num(self) -> int:
        return max(0, math.ceil(self._concurrency * 1.5 - len(self.get_unfinished_id())))

    @abstractmethod
    def get_ing_tasks(self) -> [Task]:
        pass

    @abstractmethod
    def get_todo_tasks(self) -> [Task]:
        pass

    @abstractmethod
    def get_done_tasks(self) -> [Task]:
        pass

    @abstractmethod
    def get_success_tasks(self) -> [Task]:
        pass

    def handle_task(self, task: Task) -> Error:
        logger.info(f"start handle task {task.id()} {len(self._handlers)} {self._concurrency}")
        err: Error
        err = task.ing()
        if not err.ok:
            logger.error(f'status handle task {task.id()} {err}')
            return err

        for handle in self._handlers:
            err = handle(task)
            if not err.ok:
                logger.error(f'do handle task {task.id()} {err}')
                task.fail()
                return err
        logger.info(f'end handle task {task.id()}')
        return task.success()

    def start(self, auto_clean=False):
        with Pool(processes=self._concurrency) as pool:
            for task in self.get_ing_tasks():
                task.todo(force=True)

            while True:
                for task in self.get_todo_tasks():
                    pool.apply_async(self.handle_task, args=(task,))
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
