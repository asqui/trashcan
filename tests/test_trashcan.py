from pathlib import Path
from time import time, sleep
from typing import Callable

import py
import pytest
from testfixtures import ShouldRaise, LogCapture

from trashcan import Trashcan


@pytest.fixture
def log():
    with LogCapture() as log_:
        yield log_


@pytest.fixture
def file_path(tmpdir: py.path.local) -> Path:
    path = tmpdir / 'test.txt'
    path.write_binary(b'')
    return Path(path)


@pytest.fixture
def dir_path(tmpdir: py.path.local) -> Path:
    path = tmpdir / 'foo'
    path.mkdir()
    (path / 'test1.txt').write_binary(b'')
    (path / 'test2.txt').write_binary(b'')
    return Path(path)


def assert_eventually_false(c: Callable, timeout: float = 1, poll: float = 0.01):
    start = time()
    while True:
        now = time()
        success = not c()
        if now - start > timeout or success:
            break
        sleep(poll)
    assert success


def test_assert_eventually_false():
    with ShouldRaise(AssertionError):
        assert_eventually_false(lambda: True, timeout=0.001, poll=0.001)


class Checks:

    def test_delete_str_file(self, trashcan: Trashcan, file_path: Path):
        trashcan(str(file_path))
        assert_eventually_false(file_path.exists)

    def test_delete_path_file(self, trashcan: Trashcan, file_path: Path):
        trashcan(file_path)
        assert_eventually_false(file_path.exists)

    def test_delete_str_dir(self, trashcan: Trashcan, dir_path: Path):
        trashcan(str(dir_path))
        assert_eventually_false(dir_path.exists)

    def test_delete_path_dir(self, trashcan: Trashcan, dir_path: Path):
        trashcan(dir_path)
        assert_eventually_false(dir_path.exists)

    def test_delete_not_there(self, trashcan: Trashcan, tmpdir: Path, log: LogCapture):
        path = tmpdir / 'not.there'
        trashcan(path)
        trashcan.shutdown()
        log.check(('trashcan', 'ERROR', f'Exception deleting {path}'))


class TestSimple(Checks):

    @pytest.fixture
    def trashcan(self):
        return Trashcan()


class TestThreads(Checks):

    @pytest.fixture
    def trashcan(self):
        return Trashcan(threads=1)


class TestProcesses(Checks):

    @pytest.fixture
    def trashcan(self):
        return Trashcan(processes=1)


class TestProcessesAndThreads(Checks):

    @pytest.fixture
    def trashcan(self):
        return Trashcan(processes=1, threads=1)
