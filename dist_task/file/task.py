from pathlib import Path
from typing import Optional

from common_tool.errno import Error, OK
from common_tool.log import logger
from common_tool.system import remote_file_exist, remote_create_file, remote_rm_file

from dist_task.file.common import list_dir
from dist_task.abstract.task import Task, TaskStatus, TODO, ING, SUCCESS, FAIL, DONE, INIT
from dist_task.file.config import is_remote, USER


class FileTask(Task):
    def __init__(self, ID, task_dir: Path, status_dir: Path = "", host: str = ''):
        """
        :param ID: 任务ID，根据ID可以在task_dir中找到任务信息文件/文件夹
        :param task_dir: 存放任务信息的路径，不包括任务文件/文件夹
        :param status_dir: 任务状态路径
        :param host: 远程host，远程任务
        """
        self._id = ID
        if status_dir:
            self._todo = status_dir.joinpath(f"{self._id}.{TODO}")
            self._ing = status_dir.joinpath(f"{self._id}.{ING}")
            self._success = status_dir.joinpath(f"{self._id}.{SUCCESS}")
            self._fail = status_dir.joinpath(f"{self._id}.{FAIL}")
            self._done = status_dir.joinpath(f"{self._id}.{DONE}")
            self._status_dir = status_dir
        self._host = host
        self._task_dir = task_dir

    def _is_remote(self):
        return is_remote(self._host)

    def _exist(self, to_check: Path) -> bool:
        if self._is_remote():
            return remote_file_exist(self._host, USER, to_check)
        return to_check.exists()

    def _create(self, to_create: Path):
        if self._is_remote():
            remote_create_file(self._host, USER, to_create)
        else:
            to_create.touch()

    def _rm(self, to_rm: Path):
        if self._is_remote():
            remote_rm_file(self._host, USER, to_rm)
        else:
            if to_rm.is_file():
                to_rm.unlink()
            else:
                to_rm.rmdir()

    def status(self) -> TaskStatus:
        files, _ = list_dir(self._is_remote(), self._host, USER, self._status_dir)
        status = files.get(self._id)
        if not status:
            return INIT
        if len(status) > 1:
            logger.error(f'task status err {self._id} {status}, will use first status')
        status = {s.suffix.replace('.', '') for s in status}
        for s in [TODO, ING, SUCCESS, FAIL, DONE]:
            if s in status:
                return s
        logger.error(f'task status undefined status {status}, will use INIT')
        return INIT

    def task_dir(self) -> tuple[Optional[Path], bool]:
        """
        :return: 存放任务信息的文件/文加夹，是否是文件
        """
        files, dirs = list_dir(self._is_remote(), self._host, USER, self._task_dir)
        f = files.get(self._id)
        if f:
            return f.pop(), True

        f = dirs.get(self._id)
        if f:
            return f.pop(), False

        logger.error(f'task dir {self._id} None')
        return None, False

    @property
    def id(self) -> str:
        return self._id

    def mark_todo(self, force: bool = False) -> Error:
        if force:
            for i in ((self.is_ing, self._ing),):
                if i[0]():
                    self._rm(i[1])

        self._create(self._todo)
        return OK

    def mark_ing(self) -> Error:
        self._create(self._ing)
        self._rm(self._todo)
        return OK

    def mark_success(self) -> Error:
        self._create(self._success)
        self._rm(self._ing)
        return OK

    def mark_fail(self) -> Error:
        self._create(self._fail)
        self._rm(self._ing)
        return OK

    def mark_done(self) -> Error:
        if self.is_success():
            self._rm(self._success)
        if self.is_fail():
            self._rm(self._fail)
        self._create(self._done)
        return OK

    def clean(self) -> Error:
        if not self.is_done():
            return OK
        task_dir, _ = self.task_dir()
        if task_dir:
            self._rm(task_dir)
        self._rm(self._done)
        return OK
