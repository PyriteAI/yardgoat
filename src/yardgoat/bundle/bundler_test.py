import os
from pathlib import Path
from tarfile import TarFile
import tempfile

import pytest

from . import bundler
from .exceptions import BundleConfigNotPresent


@pytest.fixture
def tempdir():
    return Path(tempfile.mkdtemp())


@pytest.fixture
def config_text():
    return """name = "test"\n"""


def test_create_bundle_no_config(tempdir):
    with pytest.raises(BundleConfigNotPresent):
        bundler.create_bundle(tempdir)

    os.rmdir(tempdir)


def test_create_bundle_config_only(tempdir, config_text):
    configfile = tempdir.joinpath(bundler.BUNDLE_CONFIG_FILENAME)

    with configfile.open("w") as f:
        f.write(config_text)

    bundle = bundler.create_bundle(tempdir)
    assert bundle.config.name == "test"
    assert bundle.config.cmd is None
    assert bundle.config.entrypoint is None
    assert not bundle.config.volumes
    with TarFile(bundle.tarfile, "r") as tar:
        assert list(tar.getnames()) == [bundler.BUNDLE_CONFIG_FILENAME]

    configfile.unlink()
    os.rmdir(tempdir)
    os.unlink(bundle.tarfile)


def test_create_bundle(tempdir, config_text):
    configfile = tempdir.joinpath(bundler.BUNDLE_CONFIG_FILENAME)

    with configfile.open("w") as f:
        f.write(config_text)

    foofile = tempdir.joinpath("foo.txt")
    with foofile.open("w") as f:
        f.write("foo\n")
    barfile = tempdir.joinpath("bar.txt")
    with barfile.open("w") as f:
        f.write("bar\n")

    bundle = bundler.create_bundle(tempdir)
    assert bundle.config.name == "test"
    assert bundle.config.cmd is None
    assert bundle.config.entrypoint is None
    assert not bundle.config.volumes
    with TarFile(bundle.tarfile, "r") as tar:
        assert set(tar.getnames()) == set(
            [bundler.BUNDLE_CONFIG_FILENAME, "foo.txt", "bar.txt"]
        )

    configfile.unlink()
    foofile.unlink()
    barfile.unlink()
    os.rmdir(tempdir)
    os.unlink(bundle.tarfile)
