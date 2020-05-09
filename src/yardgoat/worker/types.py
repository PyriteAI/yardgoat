from typing import Any, Optional, Union
from typing_extensions import Protocol
from urllib.parse import ParseResult, urlparse

import attr
import ulid

from .exceptions import AlreadyCompletedError


@attr.s(frozen=True, slots=True)
class ObjectURI:
    scheme: str = attr.ib()
    authority: str = attr.ib()
    path: str = attr.ib()

    @classmethod
    def from_parse_result(cls, pr: ParseResult) -> "ObjectURI":
        return cls(scheme=pr.scheme, authority=pr.netloc, path=pr.path)

    def to_uri_string(self) -> str:
        if self.authority:
            return f"{self.scheme}://{self.authority}{self.path}"
        else:
            return f"{self.scheme}{self.path}"


def _convert_uri(obj: Any) -> ObjectURI:
    if obj is None:
        return None
    if isinstance(obj, ObjectURI):
        return obj
    pr = urlparse(obj)
    return ObjectURI.from_parse_result(pr)


def _validate_uri(
    instance, attribute: attr.Attribute, value: Optional[ObjectURI]
) -> None:
    if value is not None and value.scheme != "s3":
        raise ValueError(f"only s3 uris are currently supported (received '{value}'")


@attr.s(frozen=True, slots=True)
class Job:
    id: str = attr.ib()
    bundle_uri: ObjectURI = attr.ib(converter=_convert_uri, validator=_validate_uri)
    output_uri: Optional[ObjectURI] = attr.ib(
        default=None, converter=_convert_uri, validator=_validate_uri
    )

    @classmethod
    def new(
        cls,
        bundle_uri: Union[str, ObjectURI],
        output_uri: Optional[Union[str, ObjectURI]] = None,
    ) -> "Job":
        return cls(ulid.new().str, bundle_uri=bundle_uri, output_uri=output_uri)

    def complete(self, output_uri: Union[str, ObjectURI]) -> "Job":
        if self.output_uri is not None:
            raise AlreadyCompletedError("this job has already been completed")
        return attr.evolve(self, output_uri=output_uri)


class JobQueue(Protocol):
    def get_open_request(self, timeout: int = 20) -> Optional[Job]:
        ...

    def submit_new_request(self, job: Job):
        ...

    def mark_completed(self, job: Job):
        ...


__all__ = ["Job", "JobQueue", "ObjectURI"]
