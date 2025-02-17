import time
import functools
from abc import ABCMeta, abstractmethod
from multiprocessing import Pool
from dist_task.utils.errno import Error
from dist_task.abstract.task import Task


class Worker(metaclass=ABCMeta):
    HANDLERS = []
    CONCURRENCY = 1

    def set_con(self, num=1):
        self.CONCURRENCY = num

    @abstractmethod
    def upload_task(self, task) -> Error:
        pass

    @abstractmethod
    def push_task(self, task) -> Error:
        pass

    @abstractmethod
    def get_unfinished_id(self) -> [str]:
        pass

    def free_num(self) -> int:
        return max(0, int(self.CONCURRENCY * 1.5 - len(self.get_unfinished_id())))

    @abstractmethod
    def get_ing_tasks(self) -> [Task]:
        pass

    @abstractmethod
    def get_todo_tasks(self) -> [Task]:
        pass

    def handle_task(self, task: Task) -> Error:
        err: Error
        err = task.ing()
        if not err.ok:
            return err

        for handle in self.HANDLERS:
            err = handle(task)
            if not err.ok:
                task.fail()
                return err

        return task.success()

    def start(self):
        with Pool(processes=self.CONCURRENCY) as pool:
            for task in self.get_ing_tasks():
                pool.apply_async(self.handle_task, task)

            while True:
                for task in self.get_todo_tasks():
                    pool.apply_async(self.handle_task, task)
                time.sleep(1)


def handler(position=0):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            Worker.HANDLERS.insert(position, func)
            return func(*args, **kwargs)

        return wrapper

    return decorator
