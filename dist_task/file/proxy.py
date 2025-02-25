from collections import defaultdict
from pathlib import Path
import threading
from copy import deepcopy

from common_tool.errno import Error, OK
from common_tool.log import logger

from dist_task.abstract.proxy import Proxy
from dist_task.abstract.task import SUCCESS


class FileProxy(Proxy):

    def __init__(self, status_dir: Path):
        self._to_pull: dict[str, set[str]] = defaultdict(set)
        self._to_pull_lock = threading.RLock()
        self._status_dir = status_dir

    def _to_pull_add(self, worker_id: str, task_id: str):
        with self._to_pull_lock:
            self._to_pull[worker_id].add(task_id)

    def _to_pull_remove(self, worker_id: str, task_id: str):
        with self._to_pull_lock:
            self._to_pull[worker_id].remove(task_id)

    def init(self) -> Error:
        super().init()
        for worker_id, _ in self.all_workers().items():
            d = self._status_dir.joinpath(worker_id)
            if not d.exists():
                d.mkdir()
                continue
            for f in d.iterdir():
                if not f.is_file():
                    continue
                self._to_pull_add(worker_id, f.stem)
        return OK

    def is_pushed(self, task_id) -> bool:
        for _, task_ids in self._to_pull.items():
            if task_id in task_ids:
                return True
        return False

    def get_to_pull(self) -> dict[str, set[str]]:
        return deepcopy(self._to_pull)

    def record_worker_task(self, task_id: str, worker_id: str) -> Error:
        self._status_dir.joinpath(worker_id, task_id).touch()
        self._to_pull_add(worker_id, task_id)
        return OK

    def remove_worker_task(self, task_id: str, worker_id: str) -> Error:
        self._to_pull_remove(worker_id, task_id)
        self._status_dir.joinpath(worker_id, task_id).unlink()
        return OK
