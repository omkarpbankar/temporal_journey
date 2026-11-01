"""Phase 16: Activities that talk to real AWS APIs through boto3.

Tests run these against moto, an in-process AWS fake, so no account is needed.
Each one is idempotent in the way that suits its service:

S3: the key comes from the run ID, so a retry overwrites the same object.
DynamoDB: a condition makes the second write a no-op.
SNS and SQS: delivery is at-least-once, so a DynamoDB marker guards the publish and
consumers must still tolerate a rare duplicate.
"""

import json
from dataclasses import dataclass

import boto3
from botocore.exceptions import ClientError
from temporalio import activity
from temporalio.exceptions import ApplicationError

NON_RETRYABLE_CODES = {"AccessDenied", "NoSuchBucket", "NoSuchKey", "ValidationException", "InvalidParameter"}


def classify(error: ClientError, operation: str) -> Exception:
    """Decide what Temporal should do about an AWS error."""
    code = error.response["Error"]["Code"]
    if code in NON_RETRYABLE_CODES:
        return ApplicationError(f"{operation}: {code}", type=f"Aws{code}", non_retryable=True)
    return error


@dataclass
class AwsResources:
    bucket: str
    table: str
    topic_arn: str
    queue_url: str
    secret_id: str
    region: str = "us-east-1"


@dataclass
class PipelineRequest:
    run_id: str
    source_key: str
    notify_address: str


@dataclass
class RawObject:
    key: str
    rows: int


@dataclass
class AwsPipelineActivities:
    resources: AwsResources

    def _client(self, service: str):
        return boto3.client(service, region_name=self.resources.region)

    def _read_rows(self, key: str) -> list[dict]:
        try:
            body = self._client("s3").get_object(Bucket=self.resources.bucket, Key=key)["Body"].read()
        except ClientError as error:
            raise classify(error, "s3:GetObject") from error
        return json.loads(body)

    @activity.defn
    def extract_to_s3(self, request: PipelineRequest) -> RawObject:
        rows = self._read_rows(request.source_key)
        key = f"raw/{request.run_id}.json"
        self._client("s3").put_object(Bucket=self.resources.bucket, Key=key, Body=json.dumps(rows))
        return RawObject(key, len(rows))

    @activity.defn
    def process(self, raw: RawObject) -> RawObject:
        rows = [{**row, "total": row["quantity"] * row["price"]} for row in self._read_rows(raw.key)]
        key = raw.key.replace("raw/", "processed/")
        self._client("s3").put_object(Bucket=self.resources.bucket, Key=key, Body=json.dumps(rows))
        return RawObject(key, len(rows))

    @activity.defn
    def validate_output(self, processed: RawObject) -> None:
        rows = self._read_rows(processed.key)
        if len(rows) != processed.rows or any(row["total"] < 0 for row in rows):
            raise ApplicationError("output failed validation", type="InvalidOutput", non_retryable=True)

    @activity.defn
    def load_table(self, processed: RawObject) -> int:
        table = boto3.resource("dynamodb", region_name=self.resources.region).Table(self.resources.table)
        written = 0
        for number, row in enumerate(self._read_rows(processed.key)):
            item = {"pk": f"{processed.key}#{number}", "total": row["total"]}
            try:
                table.put_item(Item=item, ConditionExpression="attribute_not_exists(pk)")
                written += 1
            except ClientError as error:
                if error.response["Error"]["Code"] != "ConditionalCheckFailedException":
                    raise classify(error, "dynamodb:PutItem") from error
            activity.heartbeat(number)
        return written

    @activity.defn
    def publish_completion(self, request: PipelineRequest) -> None:
        marker = boto3.resource("dynamodb", region_name=self.resources.region).Table(self.resources.table)
        try:
            marker.put_item(
                Item={"pk": f"published#{request.run_id}"},
                ConditionExpression="attribute_not_exists(pk)",
            )
        except ClientError as error:
            if error.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return
            raise classify(error, "dynamodb:PutItem") from error
        self._client("sns").publish(
            TopicArn=self.resources.topic_arn, Message=json.dumps({"run_id": request.run_id})
        )

    @activity.defn
    def notify_user(self, request: PipelineRequest) -> None:
        # The secret is read here, at the moment of use. It never enters a Workflow,
        # because everything a Workflow touches is written to history.
        secret = self._client("secretsmanager").get_secret_value(SecretId=self.resources.secret_id)
        self._client("sqs").send_message(
            QueueUrl=self.resources.queue_url,
            MessageBody=json.dumps(
                {
                    "to": request.notify_address,
                    "run_id": request.run_id,
                    "signed": bool(secret["SecretString"]),
                }
            ),
        )

    @activity.defn
    def delete_outputs(self, run_id: str) -> None:
        s3 = self._client("s3")
        for key in (f"raw/{run_id}.json", f"processed/{run_id}.json"):
            s3.delete_object(Bucket=self.resources.bucket, Key=key)
