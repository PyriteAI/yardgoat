from typing_extensions import Protocol

import attr

from ..bundle import BundleConfig, BundleInfo


@attr.s(frozen=True, slots=True)
class Artifact:
    """Represents an assembled Bundle for a given Engine runtime.
    
    Note that the original Bundle is included as a reference. This enables
    :class:`Engine` runtimes to 1) reference the configuration for execution; and 2)
    access any included files at runtime (e.g., for docker volumes).

    Attributes:
        name (str): The unique identifier of the artifact.
        bundle (BundleInfo): The Bundle from which this `Artifact` was assembled.
        metadata (dict): Any associated metadata about the artifcat.
    """

    name: str = attr.ib()
    bundle: BundleInfo = attr.ib()
    metadata: dict = attr.ib(factory=dict)


class Engine(Protocol):
    """Protocol for building and executing Bundle files.

    This protocol defines a common interface for Bundle building and execution, enabling
    technologies, such as Docker, to act as an execution runtime for Yardgoat. `Engine`
    implementations are responsible for two key steps in the Yardgoat execution
    lifecycle. First, they are responsible for converting a Yardgoat Bundle file into a
    format that the runtime can handle, which Yardgoat refers to as an :class:`Artifact`
    (e.g., a Docker image). Note that an `Artifact` is a superset of a Bundle, and
    includes a reference to the Bundle from which it was built. Second, instances must be
    able to execute the `Artifact` as configured by the associated :class:`BundleConfig`
    object. For example, if a user is running a Bundle using Docker, the Docker `Engine`
    must run the `Artifact` generated for that Bundle with the specified entrypoint,
    command, and also mount any volumes listed in the configuration.
    """

    def build(self, bundle: BundleInfo) -> Artifact:
        """Build the given Bundle for this Engine runtime environment.

        Args:
            bundle (BundleInfo): The Bundle to build.


        Returns:
            Artifact: The assembled Bundle for this Engine runtime environment.
        """
        ...

    def execute(self, artifact: Artifact):
        """Execute the :class:`Artifact` with the given configuration data.

        Args:
            artifact (Artifact): The artifact to execute.
        """
        ...


__all__ = ["Artifact", "Engine"]
