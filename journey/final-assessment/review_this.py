"""Part F: review this like a pull request. Find every problem. There are at least ten.

This file is for reading. It isn't meant to be run.
"""

import random
from dataclasses import dataclass
from datetime import datetime, timedelta

import requests
from temporalio import activity, workflow
from temporalio.common import RetryPolicy

# Every Workflow and every Activity in the company runs on this one queue.
TASK_QUEUE = "main"


@dataclass
class Order:
    id: str
    card_number: str
    amount: int


@activity.defn
async def charge(order: Order) -> str:
    # Called from an async Activity.
    response = requests.post("https://bank.example/charge", json=order.__dict__)
    return response.json()["id"]


@activity.defn
async def send_receipt(order: Order, charge_id: str) -> None:
    requests.post("https://mail.example/send", json={"order": order.id, "charge": charge_id})


@workflow.defn
class OrderWorkflow:
    def __init__(self) -> None:
        self.status = "new"

    @workflow.query
    def get_status(self) -> str:
        self.status = self.status.upper()
        return self.status

    @workflow.run
    async def run(self, order: Order) -> str:
        if datetime.now().weekday() < 5 and random.random() > 0.1:
            discount = requests.get("https://pricing.example/discount").json()["percent"]
            order.amount -= order.amount * discount // 100

        charge_id = await workflow.execute_activity(
            charge,
            order,
            retry_policy=RetryPolicy(maximum_attempts=0),
        )

        # New in this release, inserted above the existing receipt step.
        await workflow.execute_activity(
            "fraud_report",
            order,
            start_to_close_timeout=timedelta(days=30),
        )

        await workflow.execute_activity(
            send_receipt,
            args=[order, charge_id],
            start_to_close_timeout=timedelta(seconds=30),
        )
        return charge_id
