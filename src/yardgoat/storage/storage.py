from typing_extensions import Protocol

from ..bundle import BundleInfo
from ..types import PathLike

#: The unique identifier of a Bundle stored in a given :class:`StorageSystem`.
StorageID = str


class StorageSystem(Protocol):
    """Yardgoat Bundle Storage Protocol.

    Defines an interface for interacting with a Bundle storage backend, enabling Bundles
    to be uploaded, downloaded, or manipulated in some way.
    """

    def download(self, identifier: StorageID, destination: PathLike) -> BundleInfo:
        """Download a Bundle with the given identifier to the specified destination.

        Args:
            identifier (StorageID): The unique identifier of the Bundle in the storage
                system.
            destination (PathLike): The location to save the Bundle file. If the path is
                a directory, the Bundle will be saved under it with the filename
                *<sha256 hash digest>.goat*. If it is not a directory, the Bundle will be
                saved to disk with the full path name provided.

        Returns:
            BundleInfo: Metadata of the downloaded Bundle.
        """
        ...

    def upload(self, bundle: BundleInfo) -> StorageID:
        """Upload a Bundle to the storage system.

        Args:
            bundle (BundleInfo): The Bundle to upload, represented by its associated
                `BundleInfo`.
        
        Returns:
            StorageID: The unique identifier of the Bundle for that storage system.
        """
        ...


__all__ = ["StorageID", "StorageSystem"]
