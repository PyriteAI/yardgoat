from copy import deepcopy
import pathlib
from typing import Any, List, Optional, TextIO, Union

import attr
import toml

from .exceptions import InvalidKeyError, MissingKeyError


def _is_str_or_strlist(instance, attribute: attr.Attribute, value: Any):
    if value and not isinstance(value, (str, list)):
        raise TypeError(f"'{attribute.name}' must be a string or an array")
    elif value and isinstance(value, list):
        for v in value:
            if not isinstance(v, str):
                raise TypeError(
                    f"all values in the array '{attribute.name}' must be of type string"
                )


def _validate_volumes_data(instance, attribute: attr.Attribute, value: Any):
    for key, value in value.items():
        if not isinstance(key, str):
            raise TypeError(f"all keys in '{attribute.name}' must be of type str")
        if not isinstance(value, (dict, str)):
            raise TypeError(
                f"all entries in '{attribute.name}' must be of type str or dict"
            )
        if isinstance(value, dict):
            if "bind" not in value:
                raise MissingKeyError(
                    f"expected key 'bind' to be found in '{attribute.name}' value (top level key = {key})"
                )
            if not isinstance(value["bind"], str):
                raise TypeError(
                    f"value of 'bind' in {attribute.name} must be of type str (top level key = {key})"
                )
            if "mode" not in value:
                raise MissingKeyError(
                    f"expected key 'mode' to be found in '{attribute.name}' value (top level key = {key})"
                )
            if value["mode"] not in ["rw", "ro"]:
                raise ValueError(
                    f"unsupported value '{value['mode']}' for key 'bind' in '{attribute.name}' value (top level key = {key})"
                )
            for k in value:
                if k not in ["bind", "mode"]:
                    raise InvalidKeyError(
                        f"unexpected key '{k}' found in '{attribute.name}' value (top level key = {key})"
                    )


def _standardize_volumes(volumes: dict):
    volumes = deepcopy(volumes)

    for k in volumes:
        if isinstance(volumes[k], str):
            volumes[k] = {"bind": volumes[k], "mode": "rw"}
    return volumes


@attr.s(frozen=True, slots=True)
class BundleConfig:
    """Yardgoat Bundle configuration data.

    Every Yardgoat Bundle has an associated Bundle configuration file, which contains
    various directives and other metadata for Yardgoat to be able to successfully process
    and run a submitted Bundle. All Bundle configuration files are written in TOML. This
    dataclass represents an in-memory representation of a Bundle configuration file.

    Attributes:
        name (str): The name of the Bundle.
        cmd (str, optional): The command to pass to docker for execution. Defaults to
            `None`.
        entrypoint (str, optional): The entrypoint to pass to docker for execution.
            Defaults to `None`.
        volumes (dict): Volumes to mount to the docker container associated with this
            configuration. All keys should be local, relative paths for the directories
            or files to mount. The values can be the absolute path to bind to in the
            container; or another dictionary with the keys *bind* and *mode*, where
            *bind* is the absolute path to bind to, and *mode* is *rw* or *ro*.
            Defaults to an empty `dict`.
    """

    name: str = attr.ib(converter=str)
    cmd: Optional[Union[str, List[str]]] = attr.ib(
        default=None, validator=[_is_str_or_strlist]
    )
    entrypoint: Optional[Union[str, List[str]]] = attr.ib(
        default=None, validator=[_is_str_or_strlist],
    )
    volumes: dict = attr.ib(
        factory=dict,
        converter=_standardize_volumes,
        validator=[attr.validators.instance_of(dict), _validate_volumes_data],
    )

    def dumps(self) -> str:
        """Serialize this `BundleConfig` to a TOML string.

        Returns:
            str: TOML formatted configuration data.
        """
        d = attr.asdict(self)
        return toml.dumps(d)

    def dump(self, f: TextIO):
        """Serialize this `BundleConfig` as TOML to the given file-like object.

        Args:
            f (TextIO): The file-like object to which to write.
        """
        s = self.dumps()
        f.write(s)


def loads(s: str) -> BundleConfig:
    """Deserialize the TOML formatted string to a `BundleConfig` object.

    Args:
        s (str): The TOML formatted string to deserialize.
    
    Returns:
        BundleConfig: the `BundleConfig` representation.
    """
    toml_data = toml.loads(s)
    try:
        name = toml_data["name"]
    except KeyError as e:
        raise MissingKeyError("required key 'name' missing from config") from e

    return BundleConfig(
        name=name,
        cmd=toml_data.get("cmd"),
        entrypoint=toml_data.get("entrypoint"),
        volumes=toml_data.get("volumes", dict()),
    )


def load(f: TextIO) -> BundleConfig:
    """Deserialize the file-like object as a TOML string to a `BundleConfig` object.

    Args:
        f (TextIO): The file-like object from which to read.
    
    Returns:
        BundleConfig: The `BundleConfig` representation.
    """
    data = f.read()
    bc = loads(data)

    return bc


__all__ = ["BundleConfig", "load", "loads"]
