"""Lesson 4 Lab: The Cast.

This file implements the complete architecture of Temporal's cast in Python:
- Client: starts work, retrieves status. Never talks to Worker directly.
- Temporal Service: the manager with the order book. Stores Event History and manages Task Queues. NEVER cooks (never executes user code).
- Task Queue: the kitchen ticket rail. Workers poll it (pull, not push).
- Worker: your process running in your network. Polls for tasks and executes Workflow & Activity code.
- Workflow: the recipe (deterministic decisions).
- Activity: chopping the onions (real-world actions).

Run it:
    # 1. Full architecture demo (Client -> Service -> TaskQueue -> Worker):
    python the_cast.py demo ORD-CAST-1

    # 2. Lesson 4 Failure Exercise: 0 Workers alive when Client starts workflow:
    python the_cast.py zero-workers ORD-CAST-2
"""

from __future__ import annotations

import argparse
import time
from dataclasses import dataclass, field
from typing import Any, Callable

import world

ITEM = "keyboard"
AMOUNT = 100


# ==============================================================================
# Tasks & Queues (The Kitchen Ticket Rail)
# ==============================================================================

@dataclass
class WorkflowTask:
    task_id: str
    workflow_id: str
    workflow_name: str
    history: list[dict[str, Any]]


@dataclass
class ActivityTask:
    task_id: str
    workflow_id: str
    activity_name: str
    args: list[Any]
    kwargs: dict[str, Any]


class TaskQueue:
    def __init__(self, name: str):
        self.name = name
        self.workflow_tasks: list[WorkflowTask] = []
        self.activity_tasks: list[ActivityTask] = []

    def enqueue_workflow_task(self, task: WorkflowTask) -> None:
        self.workflow_tasks.append(task)
        print(f"  [Task Queue '{self.name}'] + WorkflowTask queued (Total waiting: {len(self.workflow_tasks)})")

    def enqueue_activity_task(self, task: ActivityTask) -> None:
        self.activity_tasks.append(task)
        print(f"  [Task Queue '{self.name}'] + ActivityTask queued for '{task.activity_name}' (Total waiting: {len(self.activity_tasks)})")

    def poll_workflow_task(self) -> WorkflowTask | None:
        if self.workflow_tasks:
            return self.workflow_tasks.pop(0)
        return None

    def poll_activity_task(self) -> ActivityTask | None:
        if self.activity_tasks:
            return self.activity_tasks.pop(0)
        return None


# ==============================================================================
# Temporal Service (The Manager with the Order Book)
# ==============================================================================

class TemporalService:
    def __init__(self):
        self.queues: dict[str, TaskQueue] = {}
        self.histories: dict[str, list[dict[str, Any]]] = {}
        self.executions: dict[str, dict[str, Any]] = {}
        self._task_counter = 0

    def get_or_create_queue(self, name: str) -> TaskQueue:
        if name not in self.queues:
            self.queues[name] = TaskQueue(name)
        return self.queues[name]

    def start_workflow(self, workflow_name: str, workflow_id: str, task_queue_name: str, input_data: Any) -> str:
        """Client calls this. Service schedules the work; never executes it."""
        print(f"\n[Temporal Service] Received start_workflow request for '{workflow_name}' (ID: {workflow_id})")
        queue = self.get_or_create_queue(task_queue_name)
        
        history = [
            {
                "event_id": 1,
                "event_type": "WORKFLOW_EXECUTION_STARTED",
                "workflow_name": workflow_name,
                "input": input_data,
            }
        ]
        self.histories[workflow_id] = history
        self.executions[workflow_id] = {"status": "RUNNING", "task_queue": task_queue_name}

        # Queue initial WorkflowTask for a Worker to pick up
        self._task_counter += 1
        wf_task = WorkflowTask(
            task_id=f"wft_{self._task_counter}",
            workflow_id=workflow_id,
            workflow_name=workflow_name,
            history=list(history),
        )
        queue.enqueue_workflow_task(wf_task)
        return workflow_id

    def poll_workflow_task(self, task_queue_name: str) -> WorkflowTask | None:
        queue = self.get_or_create_queue(task_queue_name)
        return queue.poll_workflow_task()

    def poll_activity_task(self, task_queue_name: str) -> ActivityTask | None:
        queue = self.get_or_create_queue(task_queue_name)
        return queue.poll_activity_task()

    def respond_workflow_task_completed(
        self,
        workflow_id: str,
        task_id: str,
        commands: list[dict[str, Any]],
    ) -> None:
        print(f"[Temporal Service] Worker completed WorkflowTask {task_id} with commands: {commands}")
        history = self.histories[workflow_id]
        execution = self.executions[workflow_id]
        queue = self.get_or_create_queue(execution["task_queue"])

        for cmd in commands:
            cmd_type = cmd["type"]
            if cmd_type == "SCHEDULE_ACTIVITY":
                act_name = cmd["activity_name"]
                history.append({
                    "event_id": len(history) + 1,
                    "event_type": "ACTIVITY_TASK_SCHEDULED",
                    "activity_name": act_name,
                })
                self._task_counter += 1
                act_task = ActivityTask(
                    task_id=f"act_{self._task_counter}",
                    workflow_id=workflow_id,
                    activity_name=act_name,
                    args=cmd.get("args", []),
                    kwargs=cmd.get("kwargs", {}),
                )
                queue.enqueue_activity_task(act_task)

            elif cmd_type == "COMPLETE_WORKFLOW":
                history.append({
                    "event_id": len(history) + 1,
                    "event_type": "WORKFLOW_EXECUTION_COMPLETED",
                    "result": cmd.get("result"),
                })
                execution["status"] = "COMPLETED"
                execution["result"] = cmd.get("result")
                print(f"[Temporal Service] Workflow {workflow_id} marked COMPLETED!")

    def respond_activity_task_completed(
        self,
        workflow_id: str,
        task_id: str,
        activity_name: str,
        result: Any,
    ) -> None:
        print(f"[Temporal Service] Worker completed ActivityTask {task_id} ('{activity_name}') with result: {result}")
        history = self.histories[workflow_id]
        execution = self.executions[workflow_id]
        queue = self.get_or_create_queue(execution["task_queue"])

        history.append({
            "event_id": len(history) + 1,
            "event_type": "ACTIVITY_TASK_COMPLETED",
            "activity_name": activity_name,
            "result": result,
        })

        # Activity completed -> Service schedules a new WorkflowTask so workflow code can advance
        self._task_counter += 1
        wf_task = WorkflowTask(
            task_id=f"wft_{self._task_counter}",
            workflow_id=workflow_id,
            workflow_name=history[0]["workflow_name"],
            history=list(history),
        )
        queue.enqueue_workflow_task(wf_task)


# ==============================================================================
# Worker (Your Process in Your Infrastructure)
# ==============================================================================

class Worker:
    def __init__(self, service: TemporalService, task_queue_name: str):
        self.service = service
        self.task_queue_name = task_queue_name
        self.activities: dict[str, Callable] = {}

    def register_activity(self, name: str, fn: Callable) -> None:
        self.activities[name] = fn

    def run_once(self) -> bool:
        """Polls for one task and executes it. Returns True if work was done."""
        # 1. Poll for WorkflowTask
        wf_task = self.service.poll_workflow_task(self.task_queue_name)
        if wf_task:
            print(f"  [Worker] Polled WorkflowTask {wf_task.task_id} for {wf_task.workflow_id}")
            commands = self._execute_workflow_logic(wf_task.history)
            self.service.respond_workflow_task_completed(wf_task.workflow_id, wf_task.task_id, commands)
            return True

        # 2. Poll for ActivityTask
        act_task = self.service.poll_activity_task(self.task_queue_name)
        if act_task:
            print(f"  [Worker] Polled ActivityTask {act_task.task_id} for '{act_task.activity_name}'")
            fn = self.activities[act_task.activity_name]
            result = fn(*act_task.args, **act_task.kwargs)
            self.service.respond_activity_task_completed(
                act_task.workflow_id, act_task.task_id, act_task.activity_name, result
            )
            return True

        return False

    def _execute_workflow_logic(self, history: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Worker replays Workflow code against history to determine next command."""
        # Determine completed activities
        completed = {
            ev["activity_name"]: ev.get("result")
            for ev in history
            if ev["event_type"] == "ACTIVITY_TASK_COMPLETED"
        }
        wf_input = history[0]["input"]
        order_id = wf_input["order_id"]

        # Step 1: create_order
        if "create_order" not in completed:
            return [{"type": "SCHEDULE_ACTIVITY", "activity_name": "create_order", "args": [order_id, ITEM, AMOUNT]}]

        # Step 2: charge_card
        if "charge_card" not in completed:
            return [{
                "type": "SCHEDULE_ACTIVITY",
                "activity_name": "charge_card",
                "args": [order_id, AMOUNT],
                "kwargs": {"idempotency_key": order_id},
            }]
        charge_id = completed["charge_card"]

        # Step 3: reserve_inventory
        if "reserve_inventory" not in completed:
            return [{"type": "SCHEDULE_ACTIVITY", "activity_name": "reserve_inventory", "args": [order_id, ITEM]}]

        # Step 4: send_email
        if "send_email" not in completed:
            return [{
                "type": "SCHEDULE_ACTIVITY",
                "activity_name": "send_email",
                "args": [order_id, f"Thanks! Your {ITEM} is on its way. Receipt: {charge_id}"],
            }]

        # All steps done
        return [{"type": "COMPLETE_WORKFLOW", "result": {"charge_id": charge_id, "status": "SUCCESS"}}]


# ==============================================================================
# Client (The Customer Placing the Order)
# ==============================================================================

class Client:
    def __init__(self, service: TemporalService):
        self.service = service

    def start_checkout(self, order_id: str, task_queue: str) -> str:
        return self.service.start_workflow(
            workflow_name="CheckoutWorkflow",
            workflow_id=order_id,
            task_queue_name=task_queue,
            input_data={"order_id": order_id},
        )

    def get_status(self, workflow_id: str) -> dict[str, Any]:
        return self.service.executions.get(workflow_id, {"status": "UNKNOWN"})


# ==============================================================================
# Demos
# ==============================================================================

def run_demo(order_id: str) -> None:
    print(f"\n==========================================")
    print(f" DEMO: The Cast in Action for {order_id}")
    print(f"==========================================")

    # 1. Initialize Temporal Service
    service = TemporalService()
    task_queue = "checkout-v1"

    # 2. Deploy Worker (Your process, your machines)
    worker = Worker(service, task_queue)
    worker.register_activity("create_order", world.create_order)
    worker.register_activity("charge_card", world.charge_card)
    worker.register_activity("reserve_inventory", world.reserve_inventory)
    worker.register_activity("send_email", world.send_email)

    # 3. Client starts workflow
    client = Client(service)
    client.start_checkout(order_id, task_queue)

    print("\n--- Worker starts polling task queue ---")
    step_count = 0
    while client.get_status(order_id)["status"] == "RUNNING" and step_count < 20:
        step_count += 1
        worker.run_once()

    print("\n--- Final Status ---")
    status = client.get_status(order_id)
    print(f"Workflow Status: {status['status']}, Result: {status.get('result')}")


def run_zero_workers_demo(order_id: str) -> None:
    print(f"\n=======================================================")
    print(f" DEMO: Lesson 4 Failure Exercise (0 Workers Deployed)")
    print(f"=======================================================")

    service = TemporalService()
    task_queue = "checkout-v1"
    client = Client(service)

    # Client starts workflow when ZERO workers exist!
    print("1. Client calls start_workflow when ZERO workers exist...")
    client.start_checkout(order_id, task_queue)

    # What is the workflow status?
    status = client.get_status(order_id)
    print(f"2. Current workflow execution status: {status['status']}")
    print(f"   Waiting tasks on TaskQueue '{task_queue}': {len(service.queues[task_queue].workflow_tasks)}")
    print("   Notice: The call SUCCEEDED! The workflow is durably scheduled and waiting.")

    print("\n3. Simulating: 1 hour later, an engineer deploys a Worker...")
    worker = Worker(service, task_queue)
    worker.register_activity("create_order", world.create_order)
    worker.register_activity("charge_card", world.charge_card)
    worker.register_activity("reserve_inventory", world.reserve_inventory)
    worker.register_activity("send_email", world.send_email)

    print("4. Worker begins polling...")
    while client.get_status(order_id)["status"] == "RUNNING":
        worker.run_once()

    print(f"5. Final workflow status: {client.get_status(order_id)['status']}")


def main():
    parser = argparse.ArgumentParser(description="Demonstrate Lesson 4: The Cast")
    subparsers = parser.add_subparsers(dest="command")

    p_demo = subparsers.add_parser("demo")
    p_demo.add_argument("order_id", default="ORD-CAST-1", nargs="?")

    p_zero = subparsers.add_parser("zero-workers")
    p_zero.add_argument("order_id", default="ORD-CAST-2", nargs="?")

    args = parser.parse_args()

    if args.command == "demo":
        run_demo(args.order_id)
    elif args.command == "zero-workers":
        run_zero_workers_demo(args.order_id)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
