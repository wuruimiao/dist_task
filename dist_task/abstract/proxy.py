from abc import ABCMeta, abstractmethod
from dist_task.abstract.worker import Worker
from common_tool.errno import Error


class Proxy(metaclass=ABCMeta):
    _workers: dict[str, Worker] = {}

    def set_workers(self, workers: [Worker]):
        self._workers = {worker.id: worker for worker in workers}

    def get_a_worker(self) -> [Worker, int]:
        for worker in self._workers.values():
            free_num = worker.free_num()
            if free_num > 0:
                return worker, free_num
        return None, 0

    def all_workers(self) -> dict[str, Worker]:
        return self._workers

    def get_the_worker(self, worker_id: str) -> Worker:
        return self._workers.get(worker_id)

    @abstractmethod
    def push_tasks(self, tasks) -> Error:
        pass

    @abstractmethod
    def pull_tasks(self,  local_dir: str) -> Error:
        pass

    @abstractmethod
    def record_worker_task(self, task, worker_id) -> Error:
        pass

    @abstractmethod
    def init(self) -> Error:
        pass
