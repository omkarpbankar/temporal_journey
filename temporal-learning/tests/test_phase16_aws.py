import json
from concurrent.futures import ThreadPoolExecutor

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws
from temporalio.client import WorkflowFailureError
from temporalio.exceptions import ApplicationError, CancelledError
from temporalio.testing import ActivityEnvironment
from temporalio.worker import Worker

from activities.aws_pipeline import AwsPipelineActivities, AwsResources, PipelineRequest, RawObject, classify
from workflows.aws_pipeline import DurablePipelineWorkflow

REGION = "us-east-1"
ROWS = [{"quantity": 2, "price": 5}, {"quantity": 1, "price": 7}]


@pytest.fixture
def aws(monkeypatch):
    for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
        monkeypatch.setenv(name, "testing")
    with mock_aws():
        s3 = boto3.client("s3", region_name=REGION)
        s3.create_bucket(Bucket="lake")
        s3.put_object(Bucket="lake", Key="source/orders.json", Body=json.dumps(ROWS))
        boto3.client("dynamodb", region_name=REGION).create_table(
            TableName="facts",
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        topic = boto3.client("sns", region_name=REGION).create_topic(Name="done")["TopicArn"]
        queue = boto3.client("sqs", region_name=REGION).create_queue(QueueName="notify")["QueueUrl"]
        boto3.client("secretsmanager", region_name=REGION).create_secret(Name="mail-key", SecretString="k")
        yield AwsResources("lake", "facts", topic, queue, "mail-key", REGION)


def worker(env, queue, resources):
    a = AwsPipelineActivities(resources)
    return Worker(
        env.client,
        task_queue=queue,
        workflows=[DurablePipelineWorkflow],
        activities=[
            a.extract_to_s3,
            a.process,
            a.validate_output,
            a.load_table,
            a.publish_completion,
            a.notify_user,
            a.delete_outputs,
        ],
        activity_executor=ThreadPoolExecutor(4),
    )


def request(run_id="run-1", source="source/orders.json"):
    return PipelineRequest(run_id, source, "ops@example.com")


def table_count():
    return boto3.resource("dynamodb", region_name=REGION).Table("facts").scan()["Count"]


async def test_full_pipeline(time_env, queue, aws):
    async with worker(time_env, queue, aws):
        result = await time_env.client.execute_workflow(
            DurablePipelineWorkflow.run, request(), id=queue, task_queue=queue
        )
    assert result == "completed"
    assert table_count() == 3  # two rows plus the publish marker
    messages = boto3.client("sqs", region_name=REGION).receive_message(QueueUrl=aws.queue_url)["Messages"]
    assert json.loads(messages[0]["Body"])["signed"] is True


async def test_bad_prefix_is_rejected_without_touching_aws(time_env, queue, aws):
    async with worker(time_env, queue, aws):
        result = await time_env.client.execute_workflow(
            DurablePipelineWorkflow.run, request(source="secret/x.json"), id=queue, task_queue=queue
        )
    assert result.startswith("rejected")


async def test_missing_source_fails_fast(time_env, queue, aws):
    async with worker(time_env, queue, aws):
        with pytest.raises(WorkflowFailureError) as error:
            await time_env.client.execute_workflow(
                DurablePipelineWorkflow.run, request(source="source/missing.json"), id=queue, task_queue=queue
            )
    assert "NoSuchKey" in str(error.value.cause.cause)


def test_load_is_idempotent(aws):
    activities = AwsPipelineActivities(aws)
    s3 = boto3.client("s3", region_name=REGION)
    s3.put_object(Bucket="lake", Key="processed/r.json", Body=json.dumps([{"total": 1}, {"total": 2}]))
    raw = RawObject("processed/r.json", 2)
    assert ActivityEnvironment().run(activities.load_table, raw) == 2
    assert ActivityEnvironment().run(activities.load_table, raw) == 0
    assert table_count() == 2


def test_publish_twice_sends_once(aws):
    activities = AwsPipelineActivities(aws)
    sqs = boto3.client("sqs", region_name=REGION)
    subscriber = sqs.create_queue(QueueName="sub")["QueueUrl"]
    arn = sqs.get_queue_attributes(QueueUrl=subscriber, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    boto3.client("sns", region_name=REGION).subscribe(TopicArn=aws.topic_arn, Protocol="sqs", Endpoint=arn)
    ActivityEnvironment().run(activities.publish_completion, request())
    ActivityEnvironment().run(activities.publish_completion, request())
    received = sqs.receive_message(QueueUrl=subscriber, MaxNumberOfMessages=10).get("Messages", [])
    assert len(received) == 1


def test_validation_rejects_bad_output(aws):
    activities = AwsPipelineActivities(aws)
    boto3.client("s3", region_name=REGION).put_object(Bucket="lake", Key="processed/x.json", Body=json.dumps([{"total": -1}]))
    with pytest.raises(ApplicationError) as error:
        ActivityEnvironment().run(activities.validate_output, RawObject("processed/x.json", 1))
    assert error.value.type == "InvalidOutput"


@pytest.mark.parametrize(
    "code, retryable",
    [("AccessDenied", False), ("NoSuchKey", False), ("ThrottlingException", True), ("InternalError", True)],
)
def test_error_classification(code, retryable):
    error = ClientError({"Error": {"Code": code, "Message": "x"}}, "Op")
    assert (classify(error, "op") is error) == retryable


def test_delete_outputs_is_safe_to_repeat(aws):
    activities = AwsPipelineActivities(aws)
    ActivityEnvironment().run(activities.delete_outputs, "never-existed")
    ActivityEnvironment().run(activities.delete_outputs, "never-existed")
