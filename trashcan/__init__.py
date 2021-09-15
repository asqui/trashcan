import shutil
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, Future
from functools import partial
from logging import getLogger
from pathlib import Path
from typing import Union, Callable, Optional

logger = getLogger(__name__)


# Holder for the thread pool used when its nested inside a process pool
_threadpool: Optional[ThreadPoolExecutor] = None


def _init_thread_pool(max_workers: int):
    global _threadpool
    _threadpool = ThreadPoolExecutor(max_workers)


def _submit(delete: Callable, path: Path):
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
            self.executor = ProcessPoolExecutor(
                max_workers=processes,
                initializer=_init_thread_pool,
                initargs=(threads,),
            )
            self.submit = partial(self.executor.submit, _submit)
        elif threads:
            self.executor = ThreadPoolExecutor(max_workers=threads)
            self.submit = self.executor.submit
        elif processes:
            self.executor = ProcessPoolExecutor(max_workers=threads)
            self.submit = self.executor.submit
        else:
            self.submit = _run

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

