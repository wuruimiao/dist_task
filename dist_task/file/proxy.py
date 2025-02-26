from pathlib import Path

from common_tool.errno import Error, OK
from common_tool.file import append_f_line

from dist_task.abstract.proxy import Proxy


class FileProxy(Proxy):
    def __init__(self, status_dir: Path, done_dir: Path):
        self._status_dir = status_dir
        self._done_dir = done_dir

    def is_pushed(self, task_id) -> bool:
        futures = [self._thread_pool.submit(worker.get_the_task, task_id) for worker in self.all_workers().values()]
        statuses = {future.result() for future in futures}
        if None in statuses:
            statuses.remove(None)
        return len(statuses) > 0

    def record_pushed_worker_task(self, task_id: str, worker_id: str) -> Error:
        self._status_dir.joinpath(worker_id).mkdir(exist_ok=True)
        self._status_dir.joinpath(worker_id, task_id).touch()
        return OK

    def record_pulled_worker_task(self, task_id: str, worker_id: str) -> Error:
        self._status_dir.joinpath(worker_id).mkdir(exist_ok=True)
        self._status_dir.joinpath(worker_id, task_id).unlink()
        append_f_line(str(self._done_dir), task_id)
        return OK
