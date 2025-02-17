from pathlib import Path
from dist_task.abstract import Worker
from dist_task.abstract.task import Task
from dist_task.file.task import FileTask
from dist_task.utils.errno import Error, OK
from dist_task.utils.cmd import run_cmd


def sync_files(local_folder, remote_folder, remote_host, remote_user):
    # 构造rsync命令
    rsync_command = [
        'rsync', '-avz', local_folder, f'{remote_user}@{remote_host}:{remote_folder}'
    ]
    run_cmd(rsync_command)

    # # 调用rsync命令
    # try:
    #     result = subprocess.run(rsync_command, check=True, capture_output=True, text=True)
    #     print(f"Rsync completed successfully:\n{result.stdout}")
    # except subprocess.CalledProcessError as e:
    #     print(f"Error occurred during rsync:\n{e.stderr}")


class FileWorker(Worker):
    def __init__(self, status_dir: str, task_dir: str, ID: str, con: int):
        self.status_dir = Path(status_dir)
        self.task_dir = Path(task_dir)
        self.ID = ID
        self.set_con(con)

    def upload_task(self, task_dir: Path) -> Error:
        if self.ID == "local":
            return OK

        sync_files(str(task_dir), str(self.task_dir), self.ID, "ubuntu")
        return OK

    def push_task(self, task) -> Error:
        task = FileTask(self.status_dir, task, self.task_dir)
        task.mark_todo()
        return OK

    def get_unfinished_id(self) -> [str]:
        return [task.id for task in self.get_all_tasks() if task.is_ing() or task.is_todo()]

    def get_all_tasks(self):
        tasks = []
        for file in self.status_dir.iterdir():
            if not file.is_file():
                continue
            tasks.append(FileTask(self.status_dir, file.stem, self.task_dir))
        return tasks

    def get_ing_tasks(self) -> [Task]:
        return [task for task in self.get_all_tasks() if task.is_ing()]

    def get_todo_tasks(self) -> [Task]:
        return [task for task in self.get_all_tasks() if task.is_todo()]
