from dist_task.abstract.proxy import Proxy
from dist_task.utils.errno import Error, OK


class FileProxy(Proxy):
    def push_tasks(self, task_dirs) -> Error:
        while len(task_dirs) > 0:
            worker, free_num = self.get_a_worker()
            if worker:
                for i in range(free_num):
                    task_dir = task_dirs.pop()
                    worker.upload_task(task_dir.resolve())
                    worker.push_task(task_dir.stem)
        return OK
