import time
import traceback
from abc import ABCMeta, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from threading import Lock

from common_tool.errno import Error, OK
from common_tool.log import logger

from dist_task.abstract.worker import Worker
from dist_task.abstract.task import Task


class Proxy(metaclass=ABCMeta):
    def __init__(self, workers: list[Worker]):
        self._workers = tuple(workers)
        self._thread_pool = None
        self._thread_pool_lock = Lock()

    @property
    def thread_pool(self) -> ThreadPoolExecutor:
        if self._thread_pool:
            return self._thread_pool
        with self._thread_pool_lock:
            if not self._thread_pool:
                self._thread_pool = ThreadPoolExecutor(max_workers=30)
            return self._thread_pool

    @property
    def workers(self) -> tuple[Worker]:
        return self._workers

    def free_workers(self) -> dict[Worker, int]:
        workers = {worker: worker.free_num() for worker in self.workers}
        return {worker: free_num for worker, free_num in workers.items() if free_num > 0}

    def get_the_worker(self, worker_id: str) -> Worker:
        for worker in self.workers:
            if worker.id() == worker_id:
                return worker
        return None

    def _push_to_worker(self, worker, task_id_storage: [tuple[str, str]]):
        oks = []
        for task_id, task_storage in task_id_storage:
            logger.info(f'start push {task_id} {task_storage} to {worker}')
            err = worker.push_task(task_id, task_storage)
            if not err.ok:
                logger.error(f'push err {task_id} {task_storage} {err}')
                continue

            self.record_pushed_worker_task(task_id, worker)
            oks.append(task_id)
            logger.info(f"end push {task_id} {task_storage} to {worker}")
        return {'worker': worker.id(), 'ok': oks}

    def push_tasks(self, task_id_storages: dict[str, Any]) -> Error:
        if len(task_id_storages) == 0:
            return OK

        to_push = []
        for worker, free_num in self.free_workers().items():
            task_id_storage: [tuple[str, str]] = []
            while free_num > 0:
                if len(task_id_storages) == 0:
                    logger.info(f'push task all pushed')
                    break
                task_id, task_storage = task_id_storages.popitem()

                if self.is_pushed(task_id):
                    logger.info(f'push ignore task {task_id} pushed')
                    continue

                free_num -= 1
                task_id_storage.append((task_id, task_storage))

            if len(task_id_storage) > 0:
                to_push.append((worker, task_id_storage))

        # 涉及子进程，.map会立即返回
        # self._thread_pool.map(lambda p: self._push_to_worker(*p), to_push)
        futures = [self.thread_pool.submit(self._push_to_worker, worker, task_id_storage)
                   for worker, task_id_storage in to_push]
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

    def pull_tasks(self, task_id_storages: dict[str, Any]) -> Error:
        futures = [self.thread_pool.submit(self._pull_from_worker, worker, task_id_storages) for worker in self.workers]
        [future.result() for future in futures]
        return OK

    def start(self, to_pull_task_id_storages: dict[str, Any], to_push_task_id_storages: dict[str, Any]) -> Error:
        from common_tool.server import MultiM

        def push(task_id_storages):
            while True:
                if len(task_id_storages) == 0:
                    logger.info(f'all task done')
                    return
                self.push_tasks(task_id_storages)
                time.sleep(3)

        MultiM.add_p('proxy_push_task', push, to_push_task_id_storages)

        def pull(task_id_storages):
            while True:
                self.pull_tasks(task_id_storages)
                time.sleep(3)

        MultiM.add_p('proxy_pull_task', pull, to_pull_task_id_storages)
        return OK

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
