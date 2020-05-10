from typing import Any, Optional, Union
from typing_extensions import Protocol
from urllib.parse import ParseResult, urlparse

import attr
import ulid

from .exceptions import AlreadyCompletedError


@attr.s(frozen=True, slots=True)
class ObjectURI:
    """Stores the core components of a URI that points to a Yardgoat object.

    The core components include the scheme (required), authority (optional), and path
    (required).

    Note: instances of this class are immutable, and as such, fields cannot be updated in
    place. You either have to manually create a new instance with the desired values, or
    use :func:`attr.evolve`.

    Attributes:
        scheme (str): The URI scheme.
        authority (str): The URI authority. According to the URI spec, this is
            optional; however, a value must be explicitly passed. Use `None` or an empty
            string to signify there's no authority.
        path (str): The URI path.
    """

    scheme: str = attr.ib()
    authority: str = attr.ib()
    path: str = attr.ib()

    @classmethod
    def from_parse_result(cls, pr: ParseResult) -> "ObjectURI":
        """Create an `ObjectURI` from a :class:`ParseResult` instance.

        The following `ParseResult` attributes are mapped to `ObjectURI` attributes
        as follows:

        - scheme --> scheme
        - netloc --> authority
        - path --> path

        All other ParseResult attributes are ignored.

        Args:
            pr (ParseResult): The `ParseResult` instance to copy.

        Returns:
            ObjectURI: The corresponding `ObjectURI` instance.
        """
        return cls(scheme=pr.scheme, authority=pr.netloc, path=pr.path)

    def to_uri_string(self) -> str:
        """Create a URI string that corresponds to the given scheme, authority, and path.

        Returns:
            str: The URI string.
        """
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
    """A Yardgoat job.

    A job represents a submission to execute a Yardgoat Bundle. This dataclass encodes
    all necesary information for a Yardgoat worker to execute a Bundle. Specifically,
    each instance contains an id, a URI that points to the Bundle to execute, and a URI
    that points to the output files (once executed).

    Note: instances of this class are immutable, and as such, fields cannot be updated in
    place. You either have to manually create a new instance with the desired values, or
    use :func:`attr.evolve`.

    Args:
        id (str): The unique id of the job.
        bundle_uri (Union[str, ObjectURI]): A URI that points to the Bundle file for this
            job. If passed as a `str`, it will be converted to an `ObjectURI`.
        output_uri (Union[str, ObjectURI], optional): A URI that points to the
            Result file for this job. If passed as a `str`, it will be converted to an
            `ObjectURI`. Defaults to `None`. Note: this should only be set once a job has
            been completed.
    
    Attributes:
        id (str): The unique id of the job.
        bundle_uri (Union[str, ObjectURI]): A URI that points to the Bundle file for this
            job.
        output_uri (Union[str, ObjectURI], optional): A URI that points to the
            Result file for this job. Defaults to `None`. Note: this should only be set
            once a job has been completed.
    """

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
        """Create a new `Job` instance with a random ULID as its id.

        Args:
            bundle_uri (Union[str, ObjectURI]): A URI that points to the Bundle file for this
            job. If passed as a `str`, it will be converted to an `ObjectURI`.
            output_uri (Union[str, ObjectURI], optional): A URI that points to the
                Result file for this job. If passed as a `str`, it will be converted to an
                `ObjectURI`. Defaults to `None`. Note: this should only be set once a job has
                been completed.
        
        Returns:
            Job: The newly created `Job` instance.
        """
        return cls(ulid.new().str, bundle_uri=bundle_uri, output_uri=output_uri)

    def complete(self, output_uri: Union[str, ObjectURI]) -> "Job":
        """Mark the job as complete by setting :attr:`Job.output_uri` to the given value.

        Note: this method does not mutate this instance, and instead returns a new
        instance of `Job`.

        Args:
            output_uri (Union[str, ObjectURI]): A URI that points to the Result file for
                this job.
        
        Returns:
            Job: a new instance with `output_uri` set to the given value.

        Raises:
            AlreadyCompletedError: raised when `output_uri` is already set.
        """
        if self.output_uri is not None:
            raise AlreadyCompletedError("this job has already been completed")
        return attr.evolve(self, output_uri=output_uri)


class JobQueue(Protocol):
    """Job queue protocol.

    Defines an interface for submitting, receiving, and marking jobs as complete.
    """

    def get_open_request(self, timeout: int = 20) -> Optional[Job]:
        """Get the next open job from the queue.

        Args:
            timeout (int, optional): The amount of time to wait for an item to be placed
                on the queue before returning.
        
        Returns:
            Job, optional: The next open job, or `None` if the queue is empty.
        """
        ...

    def submit_new_request(self, job: Job):
        """Place a job onto the queue.

        Args:
            job (Job): The job to place on the queue.
        """
        ...

    def mark_completed(self, job: Job):
        """Place a completed job on the queue to mark it as complete.

        Note: it's likely when implementing this method that a separate queue to hold
        completed jobs is needed.

        Args:
            job (Job): The job to place on the completed queue.
        """
        ...


__all__ = ["Job", "JobQueue", "ObjectURI"]
