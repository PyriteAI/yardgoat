import json
import os

import attr
import boto3
from moto import mock_sqs
import pytest

from .queues import SQSJobQueue
from .types import Job


@pytest.fixture(scope="function")
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


@pytest.fixture(scope="function")
def sqs_queue(aws_credentials):
    with mock_sqs():
        sqs = boto3.resource("sqs")
        queue = sqs.create_queue(QueueName="test")
        yield queue


@pytest.fixture(scope="function")
def sqs_job_queue(sqs_queue):
    return SQSJobQueue(sqs_queue, sqs_queue)


@pytest.fixture(scope="function")
def open_job():
    return Job.new(bundle_uri="s3://test/test")


@pytest.fixture(scope="function")
def completed_job(open_job):
    return attr.evolve(open_job, output_uri="s3://test/testtest")


def test_SQSJobQueue_submit_new_request(sqs_job_queue, open_job):
    sqs_job_queue.submit_new_request(open_job)
    msgs = sqs_job_queue.open.receive_messages(
        MessageAttributeNames=["All"], WaitTimeSeconds=10, MaxNumberOfMessages=1,
    )

    assert len(msgs) == 1

    msg = msgs[0]
    body = json.loads(msg.body)

    assert body["id"] == open_job.id
    assert body["bundle_uri"] == {
        "scheme": open_job.bundle_uri.scheme,
        "authority": open_job.bundle_uri.authority,
        "path": open_job.bundle_uri.path,
    }


def test_SQSJobQueue_get_open_request(sqs_job_queue, open_job):
    sqs_job_queue.open.send_message(MessageBody=json.dumps(attr.asdict(open_job)))

    actual = sqs_job_queue.get_open_request()
    assert actual == open_job


def test_SQSJobQueue_get_open_request_timeout(sqs_job_queue):
    actual = sqs_job_queue.get_open_request(timeout=0)
    assert actual is None


def test_SQSJobQueue_mark_completed(sqs_job_queue, completed_job):
    sqs_job_queue.mark_completed(completed_job)

    msgs = sqs_job_queue.completed.receive_messages(
        MessageAttributeNames=["All"], WaitTimeSeconds=10, MaxNumberOfMessages=1,
    )

    assert len(msgs) == 1

    msg = msgs[0]
    body = json.loads(msg.body)

    assert body["id"] == completed_job.id
    assert body["bundle_uri"] == {
        "scheme": completed_job.bundle_uri.scheme,
        "authority": completed_job.bundle_uri.authority,
        "path": completed_job.bundle_uri.path,
    }
    assert body["output_uri"] == {
        "scheme": completed_job.output_uri.scheme,
        "authority": completed_job.output_uri.authority,
        "path": completed_job.output_uri.path,
    }
