import time
from abc import ABCMeta, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from common_tool.errno import Error, OK
from common_tool.log import logger

from dist_task.abstract.worker import Worker
from dist_task.abstract.task import Task


class Proxy(metaclass=ABCMeta):
    _workers: dict[str, Worker] = {}
    _thread_pool: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=20)

    def set_workers(self, workers: [Worker]):
        self._workers = {worker.id(): worker for worker in workers}

    def free_workers(self) -> dict[Worker, int]:
        workers = {worker: worker.free_num() for worker in self._workers.values()}
        return {worker: free_num for worker, free_num in workers.items() if free_num > 0}

    def all_workers(self) -> dict[str, Worker]:
        return self._workers

    def get_the_worker(self, worker_id: str) -> Worker:
        return self._workers.get(worker_id)

    def _push_to_worker(self, worker, task_id_storage: [tuple[str, str]]):
        oks = []
        for task_id, task_storage in task_id_storage:
            err = worker.push_task(task_id, task_storage)
            if not err.ok:
                logger.error(f'push task {task_id} {task_storage} {err}')
                continue

            self.record_pushed_worker_task(task_id, worker.id())
            oks.append(task_id)
            logger.info(f"push task {task_id} to {worker.id()}")
        return {'worker': worker.id(), 'ok': oks}

    def push_tasks(self, task_id_storages: dict[str, Any]) -> Error:
        if len(task_id_storages) == 0:
            return OK
        to_push = {}
        for worker, free_num in self.free_workers().items():
            task_and_storage: [tuple[str, str]] = []
            for i in range(free_num):
                task_id, task_storage = task_id_storages.popitem()

                if self.is_pushed(task_id):
                    continue

                if worker.get_the_task(task_id):
                    continue

                task_and_storage.append((task_id, task_storage))
            to_push[worker] = task_and_storage

        futures = [self._thread_pool.submit(self._push_to_worker, worker, task_id_storage)
                   for worker, task_id_storage in to_push.items()]
        [future.result() for future in futures]
        return OK

    def _pull_from_worker(self, worker: Worker, task_id_storages: dict[str, Any]):
        ok_ids = []
        worker_id = worker.id()
        for task in worker.get_to_pull_tasks():
            task_id = task.id()
            task_storage = task_id_storages.get(task_id)
            if not task_storage:
                logger.error(f'pull task {task_id} from {worker_id} no storage')
                continue

            err = worker.pull_task(task, task_storage)
            if not err.ok:
                logger.error(f'pull task {task_id} {err}')
                continue
            logger.info(f"pull task {task_id} from {worker_id}")

            self.record_pulled_worker_task(task_id, worker_id)
            ok_ids.append(task_id)
        return ok_ids

    def pull_tasks(self, task_id_storages: dict[str, Any]) -> [str, Error]:
        futures = []
        for worker in self.all_workers().values():
            futures.append(self._thread_pool.submit(self._pull_from_worker, worker, task_id_storages))

        ok_ids = []
        [ok_ids.extend(future.result()) for future in futures]
        return ok_ids, OK

    def start(self, to_pull_task_id_storages: dict[str, Any], to_push_task_id_storages: dict[str, Any]) -> Error:
        from common_tool.server import MultiM

        def push(task_id_storages):
            while True:
                if len(task_id_storages) == 0:
                    logger.info(f'all task done')
                    break
                self.push_tasks(task_id_storages)
                time.sleep(3)
        MultiM.add_p('proxy_push_task', push, to_push_task_id_storages)

        def pull(task_id_storages):
            while True:
                self.pull_tasks(task_id_storages)
                time.sleep(3)
        MultiM.add_p('proxy_pull_task', pull, to_pull_task_id_storages)
        return OK

    def close(self):
        """
        在 Proxy 实例销毁时，清理线程池。
        """
        if self._thread_pool:
            self._thread_pool.shutdown(wait=True)

    @abstractmethod
    def is_pushed(self, task_id) -> bool:
        pass

    @abstractmethod
    def all_pushed(self) -> dict[Worker, list[Task]]:
        pass

    @abstractmethod
    def record_pushed_worker_task(self, task, worker_id) -> Error:
        pass

    @abstractmethod
    def record_pulled_worker_task(self, task_id: str, worker_id: str) -> Error:
        pass
