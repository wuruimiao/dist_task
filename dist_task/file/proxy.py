from pathlib import Path
from collections import defaultdict
from dist_task.abstract.proxy import Proxy
from dist_task.abstract.task import SUCCESS
from dist_task.file.task import FileTask
from dist_task.file.worker import FileWorker
from common_tool.errno import Error, OK
from common_tool.log import logger


class FileProxy(Proxy):
    def __init__(self, status_dir: Path):
        self._to_pull: dict[str, set[str]] = defaultdict(set)
        self._status_dir = status_dir

    def init(self) -> Error:
        for worker_id, _ in self.all_workers().items():
            d = self._status_dir.joinpath(worker_id)
            if not d.exists():
                d.mkdir()
                continue
            for f in d.iterdir():
                if not f.is_file():
                    continue
                self._to_pull[worker_id].add(f.stem)
        return OK

    def _is_pushed(self, task_id):
        for _, task_ids in self._to_pull.items():
            if task_id in task_ids:
                return True
        return False

    def record_worker_task(self, task_id: str, worker_id: str) -> Error:
        self._status_dir.joinpath(worker_id, task_id).touch()
        self._to_pull[worker_id].add(task_id)
        return OK

    def push_tasks(self, tasks: dict[str, Path]) -> Error:
        while len(tasks) > 0:
            worker: FileWorker
            worker, free_num = self.get_a_worker()
            if worker:
                for i in range(free_num):
                    task_id, task_dir = tasks.popitem()

                    if self._is_pushed(task_id):
                        continue

                    err = worker.upload_task(task_dir)
                    if not err.ok:
                        logger.error(f'push task {task_id} {task_dir} {err}')
                        continue

                    err = worker.push_task(task_id)
                    if not err.ok:
                        logger.error(f'push task {task_id} {task_dir} {err}')
                        continue

                    self.record_worker_task(task_id, worker.id)

                    logger.info(f"push task {task_id} to {worker.id}")
            else:
                break
        return OK

    def pull_tasks(self, local_dir: str) -> [str, Error]:
        ok_ids = []
        dones = defaultdict(set)
        for worker_id, task_ids in self._to_pull.items():
            worker = self.get_the_worker(worker_id)
            for task_id in task_ids:
                status, err = worker.get_task_status(task_id)
                if status == SUCCESS:
                    ok_ids.append(task_id)
                    err = worker.pull_task(task_id, local_dir)
                    if not err.ok:
                        logger.error(f'pull task {task_id} {err}')
                        continue
                    logger.info(f"pull task {task_id} from {worker.id}")
                    dones[worker_id].add(task_id)

        for worker_id, task_ids in dones.items():
            for task_id in task_ids:
                self._to_pull[worker_id].remove(task_id)
                self._status_dir.joinpath(worker_id, task_id).unlink()
        return ok_ids, OK
