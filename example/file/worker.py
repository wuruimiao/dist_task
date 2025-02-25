from config import get_workers

if __name__ == '__main__':
    worker = get_workers()["local"]
    worker.set_local()
    worker.start()
