import shutil
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, Future
from functools import partial
from logging import getLogger
from pathlib import Path
from typing import Union, Callable, Optional

logger = getLogger(__name__)


# Holders for the thread pool used when it's nested inside a process pool
_threads: Optional[int] = None
_threadpool: Optional[ThreadPoolExecutor] = None


def _submit(delete: Callable, path: Path):
    global _threads, _threadpool
    if _threadpool is None:
        _threadpool = ThreadPoolExecutor(_threads)
    return _threadpool.submit(delete, path)


def _run(delete: Callable, path: Path):
    result = Future()
    try:
        delete(path)
    except Exception as e:
        result.set_exception(e)
    else:
        result.set_result(None)
    return result


def log_exception(path: Path, future: Future):
    exception = future.exception()
    if exception is not None:
        logger.exception(f'Exception deleting {path}', exc_info=exception)


class Trashcan:

    executor = None

    def __init__(self, threads: int = None, processes: int = None):
        if processes and threads:
            global _threads
            _threads = threads
            self.executor = ProcessPoolExecutor(max_workers=processes)
            self.submit = partial(self.executor.submit, _submit)
        elif threads:
            self.executor = ThreadPoolExecutor(max_workers=threads)
            self.submit = self.executor.submit
        elif processes:
            self.executor = ProcessPoolExecutor(max_workers=threads)
            self.submit = self.executor.submit
        else:
            self.submit = _run

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown()

    def shutdown(self):
        if self.executor is not None:
            self.executor.shutdown()

    @staticmethod
    def delete(path: Path) -> None:
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()

    def __call__(self, path: Union[Path, str]):
        future: Future = self.submit(self.delete, Path(path))
        future.add_done_callback(partial(log_exception, path))

