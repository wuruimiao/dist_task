import math
import time
import traceback
from abc import ABCMeta, abstractmethod
from multiprocessing import Pool, Manager
from typing import Any, Optional, Callable, List, Tuple

from common_tool.errno import Error
from common_tool.log import logger

from dist_task.abstract.task import Task, TaskT

_DO = Error(7777, 'handle task', '任务执行异常')


class Worker(metaclass=ABCMeta):
    def __init__(self, handlers: List[Tuple[Callable[[TaskT, Any], Error], Any]],
                 con: int, free: int, auto_clean: bool):
        self._handlers = tuple(handlers)
        self._concurrency = con
        if not free:
            free = con * 1.5
        self._free = free
        self._auto_clean = auto_clean

    @property
    def handlers(self):
        return self._handlers

    @property
    def concurrency(self) -> int:
        return self._concurrency

    @property
    def free(self) -> int:
        return self._free

    @property
    def auto_clean(self) -> bool:
        return self._auto_clean

    @abstractmethod
    def id(self) -> str:
        pass

    @abstractmethod
    def info(self) -> str:
        pass

    def __str__(self):
        return self.id()

    def __repr__(self):
        return str(self)

    @abstractmethod
    def is_remote(self) -> bool:
        pass

    @abstractmethod
    def get_the_task(self, task_id: str) -> Optional[Task]:
        pass

    @abstractmethod
    def do_pull_task(self, task: Task, to_storage: Any) -> Error:
        pass

    @abstractmethod
    def do_push_task(self, task_id: id, from_storage: Any) -> tuple[Task, Error]:
        pass

    def pull_task(self, task: Task, to_storage: Any) -> Error:
        err = self.do_pull_task(task, to_storage)
        if not err.ok:
            return err
        return task.done()

    def push_task(self, task_id: str, from_storage: Any) -> Error:
        task, err = self.do_push_task(task_id, from_storage)
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
        return max(0, math.ceil(self.free - len(self.get_unfinished_id())))

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

        for handle in self.handlers:
            err = handle[0](task, *handle[1:])
            if not err.ok:
                task.fail()
                return err
        return task.success()

    def handle_task(self, task: Task, ing_num, lock) -> Error:
        err = _DO
        logger.info(f"start handle task {task.info()} handler num: {len(self.handlers)}")
        with lock:
            ing_num.value += 1

        try:
            logger.debug(f'start do {task}')
            err = self._do(task)
        except Exception:
            logger.error(f'handle task {task} exception: {traceback.format_exc()}')

        with lock:
            ing_num.value -= 1
        return err

    def start(self):
        logger.info(f'start with {self.concurrency} concurrent')
        async_results = set()

        with Manager() as manager:
            ing_num = manager.Value('i', 0)
            lock = manager.Lock()

            with Pool(processes=self.concurrency) as pool:
                for task in self.get_ing_tasks():
                    task.todo(force=True)

                # TODO: 使用callback再push
                while True:
                    limit = self.concurrency - ing_num.value
                    if limit > 0:
                        todos = self.get_todo_tasks(limit)
                        if todos:
                            logger.info(f'worker start push {todos}')
                            for task in todos:
                                async_results.add(pool.apply_async(self.handle_task, args=(task, ing_num, lock)))

                    if self.auto_clean:
                        for task in self.get_done_tasks():
                            task.clean()

                    logger.info(f'start waiting')
                    while True:
                        readies = {result for result in async_results if result.ready()}
                        async_results -= readies
                        logger.info(f'waiting get {readies}')
                        if len(readies) > 0:
                            # 必须获取结果，否则错误会被忽略
                            logger.info(str([f'{r.successful()} {r.get()}' for r in readies]))
                            break
                        time.sleep(0.5)
