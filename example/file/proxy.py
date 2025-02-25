from pathlib import Path

from common_tool.file import append_f_lines

from config import get_workers
from dist_task.file import FileProxy
from dist_task.file import FileTask

done_record = "done.txt"


def main():
    todos = [FileTask("task_id", Path("/local_task_dir"))]
    proxy = FileProxy()
    proxy.set_workers(get_workers().values())
    while len(todos) > 0:
        proxy.push_tasks(todos)
        oks: [FileTask]
        oks, err = proxy.pull_tasks("/local_task_dir")
        if not err.ok:
            print(err)
            continue
        append_f_lines(done_record, [task.id for task in oks])


if __name__ == '__main__':
    main()
