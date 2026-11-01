from temporalio.worker import Worker

from workflows import internals_lab as lab


async def run_traced(env, queue, **worker_options):
    lab.TRACE.clear()
    async with Worker(
        env.client, task_queue=queue, workflows=[lab.TracedWorkflow], activities=[lab.traced_step], **worker_options
    ):
        await env.client.execute_workflow(lab.TracedWorkflow.run, id=queue, task_queue=queue)
    return list(lab.TRACE)


async def test_cached_worker_runs_each_line_once(time_env, queue):
    trace = await run_traced(time_env, queue)
    assert trace == [f"line {n} replaying=False" for n in range(1, 5)]


async def test_without_a_cache_every_task_replays_from_the_top(time_env, queue):
    trace = await run_traced(time_env, queue, max_cached_workflows=0)
    print("OBSERVED no-cache trace:", trace)
    assert trace.count("line 1 replaying=False") == 1
    assert trace.count("line 1 replaying=True") == 3
    assert trace[-1] == "line 4 replaying=False"
