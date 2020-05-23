import os
from pathlib import Path
from queue import Queue
from tempfile import mkstemp
from threading import Thread
from unittest.mock import MagicMock, patch
import time

import pytest

from .types import Job
from .worker import Worker
from ..bundle import BundleConfig, BundleInfo
from ..engine.types import Artifact


@pytest.fixture(scope="function")
def mock_job():
    job = Job(id="test", bundle_uri="s3://test/test")
    return job


@pytest.fixture(scope="function")
def mock_bundle_config():
    return BundleConfig(name="test")


@pytest.fixture(scope="function")
def mock_bundle_info(mock_bundle_config):
    _, bundle_file = mkstemp()
    yield BundleInfo(config=mock_bundle_config, tarfile=Path(bundle_file))
    if os.path.exists(bundle_file):
        os.unlink(bundle_file)


@pytest.fixture(scope="function")
def mock_artifact(mock_bundle_info):
    return Artifact(name="test", bundle=mock_bundle_info)


@pytest.fixture(scope="function")
def mock_engine(mock_artifact):
    obj = MagicMock()
    obj.build = MagicMock()
    obj.build.return_value = mock_artifact
    obj.execute = MagicMock()
    obj.execute.side_effect = lambda: time.sleep(0.001)
    return obj


@pytest.fixture(scope="function")
def mock_queue(mock_job):
    obj = MagicMock()
    obj.get_open_request = MagicMock()
    obj.get_open_request.return_value = mock_job
    return obj


@pytest.fixture(scope="function")
def mock_storage(mock_bundle_info):
    obj = MagicMock()
    obj.download = MagicMock()
    obj.download.return_value = mock_bundle_info
    return obj


def test_Worker_execute_next_job(
    mock_engine, mock_queue, mock_storage, mock_job, mock_bundle_info, mock_artifact,
):
    worker = Worker(engine=mock_engine, queue=mock_queue, storage=mock_storage)
    worker.execute_next_job()

    mock_queue.get_open_request.assert_called()
    mock_storage.download.call_args.args == (
        mock_job.bundle_uri.path
    ) and mock_storage.download.call_args.kwargs.get("destination") is not None
    mock_engine.build.assert_called_with(mock_bundle_info)
    mock_engine.execute.assert_called_with(mock_artifact)
    assert not Path(mock_bundle_info.tarfile).exists()


def test_Worker_execute_next_job_engine_build_fail_cleanup(
    mock_engine, mock_queue, mock_storage, mock_bundle_info,
):
    def raise_error(*args, **kwargs):
        raise RuntimeError("test")

    mock_engine.build.side_effect = raise_error

    worker = Worker(engine=mock_engine, queue=mock_queue, storage=mock_storage)
    worker.execute_next_job()

    mock_engine.build.assert_called_with(mock_bundle_info)
    assert not Path(mock_bundle_info.tarfile).exists()


def test_Worker_execute_next_job_engine_execute_fail_cleanup(
    mock_engine, mock_queue, mock_storage, mock_bundle_info, mock_artifact,
):
    def raise_error(*args, **kwargs):
        raise RuntimeError("test")

    mock_engine.execute.side_effect = raise_error

    worker = Worker(engine=mock_engine, queue=mock_queue, storage=mock_storage)
    worker.execute_next_job()

    mock_engine.execute.assert_called_with(mock_artifact)
    assert not Path(mock_bundle_info.tarfile).exists()


def test_Worker_loop_with_shutdown(mock_engine, mock_queue, mock_storage):
    q = Queue()

    def mock_execute_next_job():
        q.put(True)

    with patch.object(
        Worker, "execute_next_job", side_effect=mock_execute_next_job
    ) as mock_method:
        worker = Worker(engine=mock_engine, queue=mock_queue, storage=mock_storage)
        t = Thread(target=worker.loop)
        t.start()
        q.get(timeout=2.0)
        worker.shutdown()
        t.join(timeout=2.0)

    assert not t.is_alive()
    mock_method.assert_called()
    assert worker._shutdown.is_set()
