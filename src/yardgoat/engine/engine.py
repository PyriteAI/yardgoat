from enum import Enum
from typing import Union

from .docker import DockerEngine
from .types import Engine


class Runtime(Enum):
    """Enumeration of supported execution runtime environments."""

    DOCKER = "docker"  #: Docker Engine runtime.

    @classmethod
    def from_str(cls, name: str) -> "Engine":
        lower = name.lower()
        if lower == cls.DOCKER.value:
            return cls.DOCKER
        else:
            raise ValueError(f"unknown runtime type '{name}'")


def new(runtime: Union[Runtime, str]) -> Engine:
    """Create a new :class:`Engine` with the specified runtime.

    Args:
        runtime (Union[Runtime, str]): The name of the execution runtime.

    Returns:
        Engine: An `Engine` instance for the given runtime.
    """
    if isinstance(runtime, str):
        runtime = Runtime.from_str(runtime)

    if runtime == Runtime.DOCKER:
        return DockerEngine()
    else:
        ValueError(f"unknown engine runtime'{runtime}'")


__all__ = ["new", "Runtime"]
