from collections import defaultdict
from pathlib import Path

from common_tool.errno import Error, OK

from dist_task.abstract.proxy import Proxy


class FileProxy(Proxy):
    def __init__(self, status_dir: Path):
        self._status_dir = status_dir

    def is_pushed(self, task_id) -> bool:
        for task_ids in self.get_to_pull().values():
            if task_id in task_ids:
                return True
        return False

    def get_to_pull(self) -> dict[str, set[str]]:
        to_pulls = defaultdict(set)
        for worker_id in self.all_workers().keys():
            d = self._status_dir.joinpath(worker_id)
            d.mkdir(exist_ok=True)
            for f in d.iterdir():
                if f.is_file():
                    to_pulls[worker_id].add(f.stem)
        return to_pulls

    def record_worker_task(self, task_id: str, worker_id: str) -> Error:
        self._status_dir.joinpath(worker_id).mkdir(exist_ok=True)
        self._status_dir.joinpath(worker_id, task_id).touch()
        return OK

    def remove_worker_task(self, task_id: str, worker_id: str) -> Error:
        self._status_dir.joinpath(worker_id).mkdir(exist_ok=True)
        self._status_dir.joinpath(worker_id, task_id).unlink()
        return OK
