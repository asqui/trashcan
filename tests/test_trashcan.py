import logging
from pathlib import Path
from time import time, sleep
from typing import Callable

import py
import pytest
from testfixtures import ShouldRaise, LogCapture

from trashcan import Trashcan


@pytest.fixture(autouse=True)
def ensure_no_stderr_output(capfd):
    yield
    # stderr = capfd.readouterr().err
    # if stderr:
    #     pytest.fail('Unexpected stderr:\n{}'.format(stderr))


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


def maketree(path: py.path.local, files: int, dirs: int, depth: int):
    path.mkdir()
    for i in range(1, files+1):
        (path / f'file{i}.text').write_text(str(i), 'ascii')
    if depth > 0:
        for i in range(1, dirs+1):
            maketree(path / f'dir{i}', files, dirs, depth-1)


def test_assert_eventually_false():
    with ShouldRaise(AssertionError):
        assert_eventually_false(lambda: True, timeout=0.001, poll=0.001)


class Checks:

    def test_delete_str_file(self, trashcan: Trashcan, file_path: Path):
        trashcan.delete(str(file_path))
        assert_eventually_false(file_path.exists)

    def test_delete_path_file(self, trashcan: Trashcan, file_path: Path):
        trashcan.delete(file_path)
        assert_eventually_false(file_path.exists)

    def test_delete_str_dir(self, trashcan: Trashcan, dir_path: Path):
        trashcan.delete(str(dir_path))
        assert_eventually_false(dir_path.exists)

    def test_delete_path_dir(self, trashcan: Trashcan, dir_path: Path):
        trashcan.delete(dir_path)
        assert_eventually_false(dir_path.exists)

    def test_delete_not_there(self, trashcan: Trashcan, tmpdir: py.path.local):
        path = tmpdir / 'not.there'
        with LogCapture(level=logging.ERROR) as log:
            trashcan.delete(str(path))
            trashcan.shutdown()
        log.check(('trashcan', 'ERROR', f'Exception deleting {path}'))

    def test_nested(self, trashcan: Trashcan, tmpdir: py.path.local, caplog):
        caplog.set_level(logging.DEBUG)
        path = tmpdir / 'root'
        maketree(path, files=5, dirs=3, depth=4)
        trashcan.delete(str(path))
        sleep(1)  # TODO: Implement more graceful shutdown and remove this
        trashcan.shutdown()
        assert not path.exists()


class TestSynchronous(Checks):

    @pytest.fixture
    def trashcan(self):
        with Trashcan() as trashcan:
            yield trashcan


class TestThreads(Checks):

    @pytest.fixture
    def trashcan(self):
        with Trashcan(threads=1) as trashcan:
            yield trashcan


class TestProcesses(Checks):

    @pytest.fixture
    def trashcan(self):
        with Trashcan(processes=1) as trashcan:
            yield trashcan


class TestProcessesAndThreads(Checks):

    @pytest.fixture
    def trashcan(self):
        with Trashcan(processes=1, threads=1) as trashcan:
            yield trashcan


class TestMany(Checks):

    @pytest.fixture
    def trashcan(self):
        with Trashcan(processes=4, threads=16) as trashcan:
            yield trashcan
