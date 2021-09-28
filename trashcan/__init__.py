import os
import shutil
from concurrent.futures import ThreadPoolExecutor, Future, wait
from functools import partial
from logging import getLogger
from pathlib import Path
from typing import Union

_log = getLogger(__name__)


def _log_future_exception(path: Path, future: Future):
    exception = future.exception()
    if exception is not None:
        _log.exception(f"Exception deleting {path}", exc_info=exception)


def _log_walk_exception(ex: OSError):
    _log.exception(f"Error walking {ex.path}", exc_info=ex)


class Trashcan:
    _thread_pool = None

    def __init__(self, threads: int = None, processes: int = None):
        if processes and threads:
            assert not processes, "Not supported yet"  # TODO

            # global _threads
            # _threads = threads
            # self.executor = ProcessPoolExecutor(max_workers=processes)
            # self.submit = partial(self.executor.submit, _submit)
        elif threads:
            self._thread_pool = ThreadPoolExecutor(max_workers=threads)
            self._delete = self._dispatch_delete_to_thread_pool
        elif processes:
            assert not processes, "Not supported yet"  # TODO

            # self.executor = ProcessPoolExecutor(max_workers=threads)
            # self.submit = self.executor.submit
        else:
            self._delete = self._delete_synchronous

    def delete(self, path: Union[Path, str]):
        if not isinstance(path, Path):
            path = Path(path)
        self._delete(path)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown()

    def shutdown(self):
        if self._thread_pool is not None:
            self._thread_pool.shutdown()

    @staticmethod
    def _delete_synchronous(path: Path):
        try:
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink()
        except:
            _log.exception(f"Exception deleting {path}", exc_info=True)

    def _dispatch_delete_to_thread_pool(self, path: str):
        _log.debug(f"Queue deletion of {path}")
        future = self._thread_pool.submit(self._delete_on_thread, path)
        future.add_done_callback(partial(_log_future_exception, path))
        return future

    def _delete_on_thread(self, path: str):
        if not os.path.isdir(path):
            os.unlink(path)
            return

        _log.debug(f"Handling deletion of directory {path}")
        for root, dirs, files in os.walk(path, False, _log_walk_exception):
            file_delete_futures = [
                self._dispatch_delete_to_thread_pool(os.path.join(root, file))
                for file in files
            ]

            _log.debug(f"Queue deletion of {root}")
            self._thread_pool.submit(
                self._delete_directory_when_files_have_been_deleted,
                root,
                file_delete_futures,
            ).add_done_callback(partial(_log_future_exception, root))

    def _delete_directory_when_files_have_been_deleted(self, p: str, futures):
        _log.debug(f"Waiting to delete directory {p}")
        wait(futures)
        _log.debug(f"Deleting directory {p}")
        os.rmdir(p)
