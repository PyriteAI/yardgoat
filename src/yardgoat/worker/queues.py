import json
from typing import Optional

import attr
import boto3
import ulid

from .types import *


class SQSJobQueue(JobQueue):
    """An AWS SQS backed job queue.

    This class implements the :class:`JobQueue` protocol, and utlizes AWS SQS to queue
    jobs. Note that this implementation is actually backed to two separate SQS queues:
    the first is for open jobs, and the second is for completed jobs.

    Args:
        open (boto3.resources.factory.sqs.Queue): The queue for open requests.
        completed (boto3.resources.factory.sqs.Queue): The queue for completed requests.

    Attributes:
        open (boto3.resources.factory.sqs.Queue): The queue for open requests.
        completed (boto3.resources.factory.sqs.Queue): The queue for completed requests.
    """

    def __init__(
        self,
        open: "boto3.resources.factory.sqs.Queue",
        completed: "boto3.resources.factory.sqs.Queue",
    ):
        self.open = open
        self.completed = completed

    def get_open_request(self, timeout: int = 20) -> Optional[Job]:
        messages = self.open.receive_messages(
            MessageAttributeNames=["All"],
            WaitTimeSeconds=timeout,
            MaxNumberOfMessages=1,
        )
        if not messages:
            return None

        message = messages[0]
        message.delete()  # remove from queue.
        job_json = json.loads(message.body)
        return Job(
            id=ulid.from_str(job_json["id"]),
            bundle_uri=ObjectURI(**job_json["bundle_uri"]),
        )

    def submit_new_request(self, job: Job):
        self.open.send_message(MessageBody=json.dumps(attr.asdict(job)),)

    def mark_completed(self, job: Job):
        self.completed.send_message(MessageBody=json.dumps(attr.asdict(job)),)


__all__ = ["SQSJobQueue"]
