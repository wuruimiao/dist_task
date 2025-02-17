from abc import ABCMeta, abstractmethod
from dist_task.abstract.worker import Worker
from dist_task.utils.errno import Error


class Proxy(metaclass=ABCMeta):
    WORKERS = []

    def set_workers(self, workers):
        self.WORKERS = workers

    def get_a_worker(self) -> [Worker, int]:
        for worker in self.WORKERS:
            free_num = worker.free_num()
            if free_num > 0:
                return worker, free_num
        return None, 0

    @abstractmethod
    def push_tasks(self, tasks) -> Error:
        pass
