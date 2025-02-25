from abc import ABCMeta, abstractmethod
from pathlib import Path
from dist_task.abstract.worker import Worker
from collections import defaultdict, OrderedDict

from common_tool.errno import Error, OK
from common_tool.log import logger
from concurrent.futures import ThreadPoolExecutor


class Proxy(metaclass=ABCMeta):
    _workers: dict[str, Worker] = {}
    _thread_pool: ThreadPoolExecutor = None

    def set_workers(self, workers: [Worker]):
        self._workers = {worker.id(): worker for worker in workers}

    def get_a_worker(self) -> [Worker, int]:
        # 优先远程资源
        r_worker, r_free_num = None, 0
        for worker in self._workers.values():
            free_num = worker.free_num()
            if free_num > 0:
                if worker.is_remote():
                    return worker, free_num
                else:
                    r_worker = worker
                    r_free_num = free_num
        return r_worker, r_free_num

    def all_worker_frees(self) -> dict[Worker, int]:
        workers = {worker: worker.free_num() for worker in self._workers.values()}
        return {worker: free_num for worker, free_num in workers.items() if free_num > 0}

    def all_workers(self) -> dict[str, Worker]:
        return self._workers

    def get_the_worker(self, worker_id: str) -> Worker:
        return self._workers.get(worker_id)

    @abstractmethod
    def is_pushed(self, task_id) -> bool:
        pass

    def _push_to_worker(self, worker, task_id_dirs: [tuple[str, str]]):
        """
        向指定工作者推送任务的逻辑，将任务推送过程并行化。
        """
        ok = []
        for task_id, task_dir in task_id_dirs:
            err = worker.upload_task(task_dir)
            if not err.ok:
                logger.error(f'push task {task_id} {task_dir} {err}')
                continue

            err = worker.push_task(task_id)
            if not err.ok:
                logger.error(f'push task {task_id} {task_dir} {err}')
                continue

            self.record_worker_task(task_id, worker.id())
            ok.append(task_id)
            logger.info(f"push task {task_id} to {worker.id()}")
        return {'worker': worker.id(), 'ok': ok}

    def push_tasks(self, tasks: dict[str, Path]) -> Error:
        if len(tasks) == 0:
            return OK
        to_push = {}
        for worker, free_num in self.all_worker_frees().items():
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

            self.remove_worker_task(task_id, worker.id())
            ok_ids.append(task_id)
        return ok_ids

    def pull_tasks(self, local_dir: str) -> [str, Error]:
        futures = []
        for worker_id, task_ids in self.get_to_pull().items():
            worker = self.get_the_worker(worker_id)
            futures.append(self._thread_pool.submit(self._pull_from_worker, worker, task_ids, local_dir))

        ok_ids = []
        [ok_ids.extend(future.result()) for future in futures]
        return ok_ids, OK

    @abstractmethod
    def record_worker_task(self, task, worker_id) -> Error:
        pass

    @abstractmethod
    def remove_worker_task(self, task_id: str, worker_id: str) -> Error:
        pass

    @abstractmethod
    def get_to_pull(self) -> dict[str, set[str]]:
        pass

    def init(self) -> Error:
        self._thread_pool = ThreadPoolExecutor(max_workers=len(self._workers) * 3)
        return OK

    def close(self):
        """
        在 Proxy 实例销毁时，清理线程池。
        """
        if self._thread_pool:
            self._thread_pool.shutdown(wait=True)
