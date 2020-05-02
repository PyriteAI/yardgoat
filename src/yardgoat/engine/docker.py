import pathlib
import platform
from tempfile import TemporaryDirectory
from typing import Optional

import docker

from .exceptions import MissingVolumeFile
from .types import Artifact, Engine
from ..bundle import BundleInfo, extract, extract_volume_files

YARDGOAT_DOCKER_PREFIX = "yardgoat.runner"
# We need to force TemporaryDirectory to be in /tmp for macOS.
_TMPDIR = pathlib.Path("/tmp") if platform.system() == "Darwin" else None


class DockerEngine(Engine):
    """Docker-based :class:`Engine` runtime implementation.

    This `Engine` implementation interfaces with the
    [Docker Engine](https://docs.docker.com/engine/), enabling yardgoat to execute
    Docker-based batch jobs.

    Args:
        client (docker.DockerClient): Docker client instance. Defaults to `None`, causing
            this instance to create its own client.
    
    Attributes:
        client (docker.DockerClient): The Docker client instance this instance is using.
    """

    def __init__(self, client: Optional[docker.DockerClient] = None):
        if client is None:
            client = docker.client.from_env()
        self.client = client

    def build(self, bundle: BundleInfo) -> Artifact:
        with TemporaryDirectory() as tempdir:
            extract(bundle, destination=tempdir)

            image, _ = self.client.images.build(
                path=tempdir,
                tag=f"{YARDGOAT_DOCKER_PREFIX}/{bundle.config.name}",
                labels={"goat:sha256": bundle.tarfile.stem},
                rm=True,
            )

            return Artifact(
                name=image.id,
                bundle=bundle,
                metadata={
                    "tags": image.tags,
                    "labels": image.labels,
                    "short_id": image.short_id,
                },
            )

    def execute(self, artifact: Artifact):
        with TemporaryDirectory(dir=_TMPDIR) as tempdir:
            tempdir = pathlib.Path(tempdir)
            extract_volume_files(artifact.bundle, destination=tempdir)
            volumes_absolute_paths = {}
            for k in artifact.bundle.config.volumes:
                fp = pathlib.PurePath(k)
                absolute = tempdir.joinpath(fp)
                if absolute.exists():
                    volumes_absolute_paths[absolute] = artifact.bundle.config.volumes[k]
                elif len(fp.parts) == 1:  # Create a Docker volume.
                    self.client.volumes.create(name=fp.name, driver="local")
                    volumes_absolute_paths[fp.name] = artifact.bundle.config.volumes[k]
                else:
                    raise MissingVolumeFile(
                        f"file '{fp}' does not exist in the Bundle file"
                    )

            return self.client.containers.run(
                artifact.name,
                command=artifact.bundle.config.cmd,
                remove=True,
                entrypoint=artifact.bundle.config.entrypoint,
                volumes=volumes_absolute_paths,
                stdout=True,
                stderr=True,
            )
