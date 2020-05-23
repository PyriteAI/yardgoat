from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event
from typing import Optional

from .types import Job, JobQueue
from ..engine.types import Engine
from ..storage import StorageSystem
from ..types import PathLike


class Worker:
    """Executes and manages the entire Yardgoat job lifecycle.

    `Worker` instances are responsible for executing the job lifecycle. This lifecycle
    consists of the following steps:

    1. Pull open job request from the given :class:`JobQueue`.
    2. Download the Bundle associated with the job from a given :class:`StorageSystem`.
    3. Generate an :class:`~yardgoat.engine.types.Artifact` using the specified
    :class:`Engine`.
    4. Execute the generated `Artifact`.
    5. Upload any changed/generated files located in the mounted volumes.

    This class is very flexible by design, with most of the logic handled by its
    depdencies. This allows the underlying behavior of the worker to change by simply
    replacing these dependencies with different implementations.

    Args:
        engine (Engine): Bundle execution engine.
        queue (JobQueue): Job queue.
        storage (StorageSystem): Bundle storage backend.
        workdir (PathLike, optional): Working directory. Defaults to the OS specified
            temp directory.
    """

    def __init__(
        self,
        engine: Engine,
        queue: JobQueue,
        storage: StorageSystem,
        workdir: Optional[PathLike] = None,
    ):
        self.engine = engine
        self.queue = queue
        self.storage = storage
        self.workdir = Path(workdir) if workdir else None
        self._shutdown = Event()

    def loop(self):
        """Continuously process jobs.

        Note: this method blocks.
        """
        while not self._shutdown.is_set():
            self.execute_next_job()

    def execute_next_job(self):
        """Acquire and execute the next available job."""
        if self._shutdown.is_set():
            RuntimeError("unable to process new jobs - worker shutdown")
        with TemporaryDirectory(dir=self.workdir) as workdir:
            job = self.queue.get_open_request()
            bundle_info = self.storage.download(
                job.bundle_uri.path, destination=workdir
            )
            try:
                artifact = self.engine.build(bundle_info)
                self.engine.execute(artifact)
            except Exception:
                pass  # TODO: handle
            finally:
                Path(bundle_info.tarfile).unlink()

    def shutdown(self):
        """Signal this `Worker` to stop."""
        self._shutdown.set()


__all__ = ["Worker"]
