import os
from pathlib import Path
import tempfile

import boto3
from moto import mock_s3
import pytest

from .. import bundle
from .s3 import S3Storage

TEST_BUCKET = "test"


@pytest.fixture(scope="function")
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


@pytest.fixture(scope="function")
def s3_client(aws_credentials):
    with mock_s3():
        client = boto3.client("s3")
        client.create_bucket(Bucket=TEST_BUCKET)
        yield client


@pytest.fixture(scope="function")
def s3_storage(s3_client):
    return S3Storage(s3_client, TEST_BUCKET)


@pytest.fixture(scope="function")
def example_bundle():
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir_path = Path(temp_dir)
        project_dir = temp_dir_path.joinpath("foo")
        project_dir.mkdir()
        config_file = project_dir.joinpath(bundle.BUNDLE_CONFIG_FILENAME)
        with config_file.open("w") as f:
            f.write("""name = "foo"\n""")
        info = bundle.create_bundle(project_dir)
        yield info


def test_S3Storage_upload(s3_storage, example_bundle):
    identifier = s3_storage.upload(example_bundle)

    assert identifier == example_bundle.tarfile.name

    existing_files = s3_storage.client.list_objects(Bucket=TEST_BUCKET)
    assert [identifier] == [v["Key"] for v in existing_files["Contents"]]


def test_S3Storage_download(s3_storage, example_bundle):
    s3_storage.client.upload_file(
        str(example_bundle.tarfile), TEST_BUCKET, example_bundle.tarfile.name
    )

    with tempfile.TemporaryDirectory() as tempdir:
        destination = Path(tempdir).joinpath(example_bundle.tarfile.name)
        info = s3_storage.download(example_bundle.tarfile.name, destination)

        assert info.config.name == "foo"
        assert info.tarfile == destination
