import time
from abc import ABCMeta, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from common_tool.errno import Error, OK
from common_tool.log import logger

from dist_task.abstract.worker import Worker


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

    def _push_to_worker(self, worker, task_id_dirs: [tuple[str, str]]):
        oks = []
        for task_id, task_dir in task_id_dirs:
            err = worker.upload_task(task_dir)
            if not err.ok:
                logger.error(f'push task {task_id} {task_dir} {err}')
                continue

            err = worker.push_task(task_id)
            if not err.ok:
                logger.error(f'push task {task_id} {task_dir} {err}')
                continue

            self.record_pushed_worker_task(task_id, worker.id())
            oks.append(task_id)
            logger.info(f"push task {task_id} to {worker.id()}")
        return {'worker': worker.id(), 'ok': oks}

    def push_tasks(self, tasks: dict[str, Path]) -> Error:
        if len(tasks) == 0:
            return OK
        to_push = {}
        for worker, free_num in self.free_workers().items():
            task_id_dirs: [tuple[str, str]] = []
            for i in range(free_num):
                task_id, task_dir = tasks.popitem()

                if self.is_pushed(task_id):
                    continue

                task_id_dirs.append((task_id, task_dir))
            to_push[worker] = task_id_dirs

        futures = [self._thread_pool.submit(self._push_to_worker, worker, task_id_dirs)
                   for worker, task_id_dirs in to_push.items()]
        logger.info(f'push task ok {[future.result() for future in futures]}')
        return OK

    def _pull_from_worker(self, worker: Worker, task_ids: [str], local_dir: str):
        ok_ids = []
        for task_id in task_ids:
            status, err = worker.get_task_status(task_id)
            if not status.is_success():
                continue

            err = worker.pull_task(task_id, local_dir)
            if not err.ok:
                logger.error(f'pull task {task_id} {err}')
                continue
            logger.info(f"pull task {task_id} from {worker.id()}")

            self.record_pulled_worker_task(task_id, worker.id())
            ok_ids.append(task_id)
        return ok_ids

    def pull_tasks(self, local_dir: str) -> [str, Error]:
        futures = []
        for worker_id, task_ids in self.get_to_pulls().items():
            worker = self.get_the_worker(worker_id)
            futures.append(self._thread_pool.submit(self._pull_from_worker, worker, task_ids, local_dir))

        ok_ids = []
        [ok_ids.extend(future.result()) for future in futures]
        return ok_ids, OK

    def start(self, local_dir: str, tasks: dict[str, Path]) -> Error:
        from common_tool.server import MultiM

        def push(_tasks):
            while True:
                if len(_tasks) == 0:
                    logger.info(f'all task done')
                    break
                self.push_tasks(_tasks)
                time.sleep(3)
        MultiM.add_p('proxy_push_task', push, tasks)

        def pull(_local_dir):
            while True:
                self.pull_tasks(_local_dir)
                time.sleep(3)
        MultiM.add_p('proxy_pull_task', pull, local_dir)
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
    def record_pushed_worker_task(self, task, worker_id) -> Error:
        pass

    @abstractmethod
    def record_pulled_worker_task(self, task_id: str, worker_id: str) -> Error:
        pass

    @abstractmethod
    def get_to_pulls(self) -> dict[str, set[str]]:
        pass
