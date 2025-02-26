from pathlib import Path

from common_tool.errno import Error, OK
from common_tool.file import append_f_line

from dist_task.abstract.proxy import Proxy
from dist_task.file.worker import FileWorker
from dist_task.file.task import FileTask


class FileProxy(Proxy):
    def __init__(self, done_dir: Path):
        self._done_dir = done_dir

    def is_pushed(self, task_id) -> bool:
        futures = [self._thread_pool.submit(worker.get_the_task, task_id) for worker in self.all_workers().values()]
        tasks = {future.result() for future in futures}
        if None in tasks:
            tasks.remove(None)
        return len(tasks) > 0

    def all_pushed(self) -> dict[FileWorker, list[FileTask]]:
        worker: FileWorker
        futures = {worker: self._thread_pool.submit(worker.get_all_tasks)
                   for worker in self.all_workers().values()}
        futures = {worker: future.result() for worker, future in futures.items()}
        return {
            worker: [task for tasks in storage_tasks.values() for task in tasks]
            for worker, storage_tasks in futures.items()
        }

    def record_pushed_worker_task(self, task_id: str, worker_id: str) -> Error:
        return OK

    def record_pulled_worker_task(self, task_id: str, worker_id: str) -> Error:
        append_f_line(str(self._done_dir), task_id)
        return OK
