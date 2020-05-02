import os
from pathlib import Path
import random
from tempfile import TemporaryDirectory

import docker
import pytest

from .docker import DockerEngine, YARDGOAT_DOCKER_PREFIX
from ..bundle import BundleInfo, create_bundle


@pytest.fixture(scope="module")
def docker_client():
    return docker.client.from_env()


@pytest.fixture(scope="function")
def example_bundle():
    with TemporaryDirectory() as tempdir:
        with TemporaryDirectory(dir=tempdir) as projectdir:
            projectdir = Path(projectdir)
            with projectdir.joinpath("main.sh").open("w") as f:
                f.write("echo 'Hello World!'")
            with projectdir.joinpath("Dockerfile").open("w") as f:
                f.write(
                    """FROM alpine:latest
COPY main.sh /opt/bin/

CMD ["/bin/sh", "/opt/bin/main.sh"]
"""
                )
            with projectdir.joinpath("yardgoat.toml").open("w") as f:
                f.write(f"""name = "{projectdir.name}"\n""")

            yield create_bundle(projectdir)


@pytest.fixture(scope="function")
def example_bundle_with_mount():
    with TemporaryDirectory() as tempdir:
        with TemporaryDirectory(dir=tempdir) as projectdir:
            projectdir = Path(projectdir)
            with projectdir.joinpath("data.txt").open("w") as f:
                f.write("Hello World!\n")
            with projectdir.joinpath("main.sh").open("w") as f:
                f.write("cat /data.txt")
            with projectdir.joinpath("Dockerfile").open("w") as f:
                f.write(
                    """FROM alpine:latest
COPY main.sh /opt/bin/

CMD ["/bin/sh", "/opt/bin/main.sh"]
"""
                )
            with projectdir.joinpath("yardgoat.toml").open("w") as f:
                f.write(
                    f"""
name = "{projectdir.name}"

[volumes]
"data.txt" = "/data.txt"
"""
                )

            yield create_bundle(projectdir)


@pytest.fixture(scope="function")
def example_bundle_with_docker_volume():
    with TemporaryDirectory() as tempdir:
        with TemporaryDirectory(dir=tempdir) as projectdir:
            projectdir = Path(projectdir)
            with projectdir.joinpath("main.sh").open("w") as f:
                f.write("touch /data/output.txt\n")
                f.write("echo 'Hello World!' > '/data/output.txt'\n")
                f.write("cat /data/output.txt")
            with projectdir.joinpath("Dockerfile").open("w") as f:
                f.write(
                    """FROM alpine:latest
COPY main.sh /opt/bin/

CMD ["/bin/sh", "/opt/bin/main.sh"]
"""
                )
            with projectdir.joinpath("yardgoat.toml").open("w") as f:
                f.write(
                    f"""
name = "{projectdir.name}"

[volumes]
{projectdir.name} = "/data/"
"""
                )

            yield create_bundle(projectdir)


def test_DockerEngine_build(docker_client, example_bundle):
    engine = DockerEngine(docker_client)

    artifact = engine.build(example_bundle)

    try:
        images = docker_client.images.list(
            f"{YARDGOAT_DOCKER_PREFIX}/{example_bundle.config.name}"
        )
        assert len(images) == 1

        image = images[0]
        assert image.id == artifact.name
        assert artifact.metadata["tags"] == image.tags
        assert artifact.metadata["labels"] == image.labels
        assert artifact.metadata["short_id"] == image.short_id
        assert artifact.bundle == example_bundle
    finally:
        docker_client.images.remove(artifact.name)


def test_DockerEngine_execute(docker_client, example_bundle):
    engine = DockerEngine(docker_client)

    artifact = engine.build(example_bundle)

    try:
        out = engine.execute(artifact)
        assert out and out.decode("utf-8") == "Hello World!\n"
    finally:
        docker_client.images.remove(artifact.name)


def test_DockerEngine_execute_mount(docker_client, example_bundle_with_mount):
    engine = DockerEngine(docker_client)

    artifact = engine.build(example_bundle_with_mount)

    try:
        out = engine.execute(artifact)
        assert out and out.decode("utf-8") == "Hello World!\n"
    finally:
        docker_client.images.remove(artifact.name)


def test_DockerEngine_execute_docker_volume(
    docker_client, example_bundle_with_docker_volume
):
    print(example_bundle_with_docker_volume)
    engine = DockerEngine(docker_client)

    artifact = engine.build(example_bundle_with_docker_volume)

    try:
        out = engine.execute(artifact)
        assert out and out.decode("utf-8") == "Hello World!\n"

        volumes = docker_client.volumes.list()
        assert any(v.name == artifact.bundle.config.name for v in volumes)
    finally:
        for v in docker_client.volumes.list():
            if v.name == artifact.bundle.config.name:
                v.remove()
        docker_client.images.remove(artifact.name)
