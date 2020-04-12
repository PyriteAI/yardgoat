import hashlib
import pathlib
from tarfile import TarFile, TarInfo
import tempfile
from typing import Optional
import uuid

import attr

from .config import BundleConfig, load, loads
from .exceptions import BundleConfigNotPresent
from ..types import PathLike

#: The name of all Yardgoat Bundle configuration files.
BUNDLE_CONFIG_FILENAME = "yardgoat.toml"


def _default_filter(tarinfo: TarInfo) -> Optional[TarInfo]:
    path = pathlib.Path(tarinfo.name)
    return None if ".git" in path.parts and tarinfo.isdir() else tarinfo


@attr.s(frozen=True, slots=True)
class BundleInfo:
    """Contains metadata associated with a Bundle.

    Metadata includes the associated :class:`BundleConfig` and the path to the tar file
    containing all Bundle files.

    Attributes:
        config (BundleConfig): Configuration data for the Bundle.
        tarfile (pathlib.PurePath): The path to the Bundle tar file.
    """

    @classmethod
    def from_bundle_file(cls, path: PathLike) -> "BundleInfo":
        with TarFile(path, mode="r") as f:
            try:
                reader = f.extractfile(BUNDLE_CONFIG_FILENAME)
            except KeyError as e:
                raise BundleConfigNotPresent(
                    f"invalid Bundle - no {BUNDLE_CONFIG_FILENAME} file found"
                ) from e
            else:
                data = reader.read().decode("utf-8")
                config = loads(data)
                return cls(config, pathlib.Path(path))

    config: BundleConfig = attr.ib()
    tarfile: pathlib.PurePath = attr.ib()


def create_bundle(path: PathLike) -> BundleInfo:
    """Create a Yardgoat Bundle from the given directory.

    The directory at `path` must have a valid `yardgoat.toml` file present for this
    function to work. Note that currently all files and folders are added to the Bundle,
    *except* for any `.git` directories.

    Args:
        path (PathLike): The path of the directory to bundle.

    Returns:
        BundleInfo: An instance of BundleInfo containing metadata for the new Bundle.
    """
    path = pathlib.Path(path)
    config_path = path.joinpath(BUNDLE_CONFIG_FILENAME)
    try:
        with config_path.open() as f:
            config = load(f)
    except FileNotFoundError as e:
        raise BundleConfigNotPresent(
            f"no {BUNDLE_CONFIG_FILENAME} found in directory '{path}'"
        ) from e
    else:
        temptar = pathlib.Path(tempfile.gettempdir(), f"{uuid.uuid4()}.tar")
        with TarFile(temptar, mode="w") as tar:
            for p in path.iterdir():
                tar.add(str(p), p.name, filter=_default_filter)

        hex = hashlib.sha256()
        with temptar.open("rb") as f:
            buf = f.read(65536)
            while len(buf) != 0:
                hex.update(buf)
                buf = f.read(65536)

        tarfile = pathlib.Path(temptar.parent, f"{hex.hexdigest()}.goat")
        temptar.rename(tarfile)

        return BundleInfo(config=config, tarfile=tarfile)


__all__ = ["BUNDLE_CONFIG_FILENAME", "BundleInfo", "create_bundle"]
