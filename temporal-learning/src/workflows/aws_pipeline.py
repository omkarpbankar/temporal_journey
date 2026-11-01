"""Phase 16: a durable pipeline across S3, DynamoDB, SNS, SQS, and Secrets Manager."""

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, is_cancelled_exception

with workflow.unsafe.imports_passed_through():
    from activities.aws_pipeline import AwsPipelineActivities, PipelineRequest, RawObject

TASK_QUEUE = "aws-pipeline"
AWS_CALL = dict(
    start_to_close_timeout=timedelta(minutes=2),
    retry_policy=RetryPolicy(
        initial_interval=timedelta(milliseconds=200),
        maximum_interval=timedelta(seconds=30),
        maximum_attempts=5,
    ),
)


@workflow.defn
class DurablePipelineWorkflow:
    def __init__(self) -> None:
        self.stage = "received"

    @workflow.query
    def current_stage(self) -> str:
        return self.stage

    @workflow.run
    async def run(self, request: PipelineRequest) -> str:
        if not request.source_key.startswith("source/"):
            self.stage = "rejected"
            return "rejected: source must be under source/"

        try:
            self.stage = "extracting"
            raw: RawObject = await workflow.execute_activity_method(
                AwsPipelineActivities.extract_to_s3, request, **AWS_CALL
            )
            self.stage = "processing"
            processed: RawObject = await workflow.execute_activity_method(
                AwsPipelineActivities.process, raw, **AWS_CALL
            )
            self.stage = "validating"
            await workflow.execute_activity_method(AwsPipelineActivities.validate_output, processed, **AWS_CALL)
            self.stage = "loading"
            await workflow.execute_activity_method(
                AwsPipelineActivities.load_table,
                processed,
                heartbeat_timeout=timedelta(seconds=30),
                **AWS_CALL,
            )
        except (asyncio.CancelledError, ActivityError) as error:
            if not is_cancelled_exception(error):
                raise
            self.stage = "cancelled"
            # shield keeps the cleanup running even though this Workflow was cancelled.
            await asyncio.shield(
                workflow.execute_activity_method(
                    AwsPipelineActivities.delete_outputs, request.run_id, **AWS_CALL
                )
            )
            raise

        self.stage = "publishing"
        await workflow.execute_activity_method(AwsPipelineActivities.publish_completion, request, **AWS_CALL)
        self.stage = "notifying"
        await workflow.execute_activity_method(AwsPipelineActivities.notify_user, request, **AWS_CALL)
        self.stage = "completed"
        return "completed"
