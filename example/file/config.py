from common_tool.errno import Error, OK
from dist_task.file import FileTask
from dist_task.file import FileWorker


def get_workers():
    worker_config = {
        "local": {
            "status": "/status",
            "task": "/task",
            "con": 1,
        },
        "221.112.31.1": {
            "status": "/status",
            "task": "/task",
            "con": 6,
        },
    }

    workers: [str, FileWorker] = {worker_id: FileWorker(config["status"], config["task"], worker_id, config["con"])
                                  for worker_id, config in worker_config.items()}
    for worker in workers.values():
        worker.set_handler(do)
    return workers


def do(task: FileTask) -> Error:
    print(f"do {task.id}")
    return OK
