import functools
import math
import time
import traceback
from abc import ABCMeta, abstractmethod
from multiprocessing import Pool, Manager
from typing import Any, Optional

from common_tool.errno import Error
from common_tool.log import logger

from dist_task.abstract.task import Task, TaskStatus, INIT

_DO = Error(7777, 'handle task', '任务执行异常')


class Worker(metaclass=ABCMeta):
    _handlers = []
    _concurrency = 1
    _free = 2

    @abstractmethod
    def id(self) -> str:
        pass

    def __str__(self):
        return self.id()

    def __repr__(self):
        return self.id()

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
    def get_the_task(self, task_id: str) -> Optional[Task]:
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

    # def get_task_status(self, task_id: str) -> TaskStatus:
    #     task = self.get_the_task(task_id)
    #     if not task:
    #         return INIT
    #     return task.status()

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

    def handle_task(self, task: Task, ing_num, lock) -> Error:
        logger.info(f"start handle task {task} handler num: {len(self._handlers)}")
        err = _DO
        with lock:
            ing_num.value += 1

        try:
            err = self._do(task)
        except Exception:
            logger.error(f'handle task {task} exception: {traceback.format_exc()}')

        with lock:
            ing_num.value -= 1
        return err

    def start(self, auto_clean=False):
        logger.info(f'start with {self._concurrency} concurrent')
        async_results = set()

        with Manager() as manager:
            ing_num = manager.Value('i', 0)
            lock = manager.Lock()

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
                                async_results.add(pool.apply_async(self.handle_task, args=(task, ing_num, lock)))

                    if auto_clean:
                        for task in self.get_done_tasks():
                            task.clean()

                    while True:
                        readies = {result for result in async_results if result.ready()}
                        async_results -= readies
                        if len(readies) > 0:
                            break
                        time.sleep(0.5)


def handler(position=0):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            Worker.set_handler(func, position)
            return func(*args, **kwargs)

        return wrapper

    return decorator
