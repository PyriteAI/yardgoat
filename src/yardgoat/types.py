import os

from typing import Any, Union

PathLike = Union[str, "os.PathLike[str]"]  #: File system path type.

__all__ = ["PathLike"]
