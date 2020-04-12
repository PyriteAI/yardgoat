import boto3

from .storage import StorageID, StorageSystem
from ..bundle import BundleInfo
from ..types import PathLike


class S3Storage(StorageSystem):
    def __init__(self, client: "botocore.client.S3", bucket: str):
        self.client = client
        self.bucket = bucket

    def download(self, identifier: StorageID, destination: PathLike) -> BundleInfo:
        self.client.download_file(self.bucket, str(identifier), str(destination))
        return BundleInfo.from_bundle_file(destination)

    def upload(self, bundle: BundleInfo) -> StorageID:
        self.client.upload_file(str(bundle.tarfile), self.bucket, bundle.tarfile.name)
        return bundle.tarfile.name


__all__ = ["S3Storage"]
