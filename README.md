# Celery Learning Project

A hands-on learning project exploring **Celery** with **Redis** as the broker/result backend, and **FastAPI** as the web framework triggering background tasks.

---

## 1. Understanding of Celery

**What is Celery?**
Celery is a distributed task queue for Python. It lets you run functions ("tasks") asynchronously, outside the request/response cycle of a web app — so a slow operation (sending an email, processing a file, calling a third-party API) doesn't block your API from responding immediately.

**Core components:**

| Component | Role |
|---|---|
| **Producer** | Your app (e.g., FastAPI) that calls `.delay()` / `.apply_async()` to schedule a task |
| **Broker** | Message queue (Redis, in this project) where task messages wait until a worker picks them up |
| **Worker** | A separate process that listens to the broker, picks up tasks, and executes them |
| **Result backend** | Where task results/status get stored after execution (Redis, separate DB in this project) |

**Why Redis, specifically?**
Redis can act as both broker and result backend. It's simple to run locally and fast, though (as covered below) it doesn't have native priority queues the way RabbitMQ does — Celery emulates priority on top of Redis when needed.

**The AMQP model Celery is built on (via Kombu):**
Even though we're using Redis, Celery's internal messaging model follows AMQP concepts:

```
Task published → Exchange → (matched by routing key) → Queue → Worker consumes
```

- **Exchange** — a routing post office; messages are published here, not directly to a queue.
- **Routing key** — a label on the message saying which queue(s) should receive it.
- **Queue** — the actual "mailbox" a worker listens to, bound to an exchange with a specific routing key.

Understanding this model matters once you start declaring multiple queues — see Section 4.

---

## 2. Project Setup

This project uses **Poetry** for dependency/environment management.

### Prerequisites
- Python installed
- Poetry installed
- Redis installed and running locally

### Install dependencies
```bash
poetry install
```

### Start Redis
```bash
redis-server
```

Check Redis is running and connect to it via CLI:
```bash
redis-cli
```

### Run the Celery worker
```bash
poetry run celery -A celery_learning.celery_app worker --loglevel=info
```

Run a worker listening to specific queues (see Section 4 for why this matters):
```bash
poetry run celery -A celery_learning.celery_app worker -Q high,low --loglevel=info
```

Run with a specific concurrency (number of parallel worker processes):
```bash
poetry run celery -A celery_learning.celery_app worker -Q high,low --concurrency=4 --loglevel=info
```

### Run the FastAPI app
```bash
poetry run uvicorn celery_learning.main:app --reload
```

(Adjust `celery_learning.main:app` to match your actual FastAPI entrypoint module/path.)

### Trigger tasks
With both Redis, the Celery worker, and FastAPI running:
```bash
curl -X POST http://127.0.0.1:8000/register
curl -X POST http://127.0.0.1:8000/payment
```

---

## 3. Inspecting Celery & Redis

### Celery's own inspection commands
These talk to live workers directly (more reliable/decoded than reading raw Redis):

```bash
# Tasks currently executing
celery -A celery_learning.celery_app inspect active

# Tasks fetched by a worker but not yet started
celery -A celery_learning.celery_app inspect reserved

# Tasks scheduled for later (ETA / countdown)
celery -A celery_learning.celery_app inspect scheduled

# Worker stats (pool type, concurrency, etc.)
celery -A celery_learning.celery_app inspect stats
```

### Checking a specific task's result programmatically
```python
from celery.result import AsyncResult
from celery_learning.celery_app import app

result = AsyncResult("task-id-here", app=app)
print(result.status)   # PENDING / STARTED / SUCCESS / FAILURE
print(result.result)   # return value, once available
```

### Redis CLI — inspecting the broker (db 0)
```bash
redis-cli -n 0
> KEYS *              # list all keys (queue names) in the broker db
> LLEN celery         # length of the default queue
> LLEN high           # length of the "high" queue
> LLEN low            # length of the "low" queue
> LRANGE high 0 -1    # view raw queued task messages
```

### Redis CLI — inspecting the result backend (db 1)
```bash
redis-cli -n 1
> KEYS *              # shows keys like celery-task-meta-<task-id>
> GET celery-task-meta-<task-id>   # raw JSON result for a specific task
```

**Important gotcha learned:** stored *results* (db 1) and the actual *queue* (db 0) are two completely different things. Checking `LLEN celery` on the wrong DB will always show `0` and looks like "nothing is queued" even when tasks are actively running — always confirm which DB your `broker_url` vs `result_backend` actually points to.

**Another gotcha:** a worker's `prefetch` behavior means tasks can leave the Redis list almost instantly (moving into the worker's internal "reserved" state) even before they're actually executed — so `LLEN` can show `0` while tasks are still waiting to run inside the worker itself. Use `inspect reserved` / `inspect active` to see that state.

---

## 4. Queues, Routing, `delay()` vs `apply_async()`, and Config Explained

### Why multiple queues?
A single shared queue means slow/low-priority tasks (e.g., `welcome_email`) can sit in front of urgent tasks (e.g., `payment_email`) simply because of submission order. Multiple queues let you:
- Prioritize which task type gets picked up first
- Scale worker capacity independently per task type (e.g., more workers for payments, fewer for welcome emails)
- Isolate failures (a broken task type doesn't block an unrelated pipeline)
- Get clearer operational visibility (`LLEN high` vs `LLEN low` tells you exactly where a backlog is)

### `delay()` vs `apply_async()`
```python
welcome_email.delay()                                   # shorthand, no extra options
welcome_email.apply_async(queue="low", priority=9)       # full control: queue, priority, countdown, eta, etc.
```
`.delay()` is sugar for `.apply_async()` with no extra arguments — it always falls back to whatever `task_routes` / `task_default_queue` says, since it has no way to pass overrides. `.apply_async()` lets you explicitly override the queue, set a `priority`, delay execution (`countdown=`, `eta=`), and more, on a per-call basis. **An explicit `queue=` in `apply_async()` always wins over `task_routes`.**

### Two ways to implement priority

**A. Multiple named queues + queue-check order (used in this project)**
```python
celery -A celery_learning.celery_app worker -Q high,low --loglevel=info
```
The worker checks `high` before `low` whenever it's free to pick up a new task. Strong, predictable ordering; supports independent scaling and failure isolation. Requires each queue to have its **own exchange and routing key** (see the config gotcha below) — otherwise queues can silently collide.

**B. Single queue + native `priority=` (Redis-emulated)**
```python
task_queues = (Queue("emails", queue_arguments={"x-max-priority": 10}),)
payment_email.apply_async(queue="emails", priority=0)   # 0 = highest
welcome_email.apply_async(queue="emails", priority=9)   # higher number = lower priority
```
Simpler setup, but Redis has no native priority queues — Celery emulates it, so ordering is best-effort rather than guaranteed, and it only affects ordering *within* one queue during real contention (when more tasks are queued than free worker slots).

### Config file explained (`celery_config.py`)

```python
broker_url = "redis://localhost:6379/0"   # the message queue (db 0)
result_backend = "redis://localhost:6379/1"  # where results are stored (db 1) — kept separate from the broker deliberately

task_serializer = "json"     # Redis only stores bytes/strings, so task args must be serialized
result_serializer = "json"   # same reasoning, for stored results
accept_content = ["json"]    # worker only accepts json-serialized messages (security/consistency)

timezone = "Asia/Kolkata"    # used by Celery Beat for scheduling
enable_utc = True            # internally stores/processes time in UTC, converts to `timezone` when needed

include = [
    "celery_learning.workers.tasks"   # ensures Celery imports/registers tasks defined in this module
]

task_track_started = True    # updates task state to STARTED (not just PENDING) once a worker begins it
result_expires = 3600        # stored results auto-expire after 1 hour, to avoid unbounded Redis growth

task_queues = (
    Queue("high", Exchange("high"), routing_key="high"),
    Queue("low", Exchange("low"), routing_key="low"),
)
```

**Why exchange/routing_key are set explicitly (a real bug hit during this project):**
Declaring `Queue("high")` without an explicit exchange can cause it to inherit `task_default_exchange` / `task_default_routing_key`, which in turn defaults to whatever `task_default_queue` is set to. In this project, `task_default_queue = "low"` caused **both** `high` and `low` to silently bind to the same exchange/key (`low`), meaning routing wasn't actually isolating anything. Declaring `Exchange("high")` / `routing_key="high"` explicitly for each queue removes that ambiguity entirely.

```python
task_default_queue = "low"   # fallback if a task has no matching rule in task_routes

task_routes = {
    "celery_learning.workers.tasks.payment_email": {"queue": "high"},
    "celery_learning.workers.tasks.welcome_email": {"queue": "low"},
}
```
`task_routes` keys must exactly match the task's internal name (`<module_path>.<function_name>`) — verify with `task_name.name` in Python if unsure, rather than guessing the string.

---

## 5. What I Learned

- The difference between a task **completing its HTTP request** vs. **actually executing** — `.delay()` returns instantly; the real work happens later, in a separate worker process.
- How to distinguish the **broker** (queue, db 0) from the **result backend** (stored outcomes, db 1) — and why checking the wrong Redis DB makes a fully working system look broken.
- Why `LLEN` on a queue can show `0` even while tasks are actively running, due to worker **prefetching** pulling tasks off the list before they're actually executed.
- The real mechanics of Celery routing: exchanges, routing keys, and queues aren't just naming — they determine whether messages actually land where you expect, and misconfigured defaults can silently cause queue collisions.
- Two different ways to implement task prioritization (multiple queues vs. single queue + native priority), and the tradeoffs between them on a Redis backend specifically.
- The precedence order between `task_routes` (global default) and `apply_async(queue=...)` (explicit override).
- Concurrency in Celery's default `prefork` pool means separate OS processes running in true parallel — not `asyncio`-style cooperative concurrency. Async-style concurrency requires switching to `gevent`/`eventlet` pools instead.
- Debugging methodology: reading worker logs precisely (matching "received" and "succeeded" timestamps *per task*) to distinguish real blocking/priority issues from simple submission-timing differences.
- Some Git fundamentals resurfaced along the way — commit `author` vs `committer` fields, `--amend --reset-author`, and how GitHub's repo-level contributor stats can lag behind a rewritten/force-pushed history even when the actual commit data is clean.

---

## Project Structure (reference)
```
celery_learning/
├── src/
│   └── celery_learning/
│       ├── __init__.py
│       ├── celery_app.py          # Celery app instance, loads config via app.config_from_object(...)
│       ├── main.py                # FastAPI app exposing /register and /payment
│       ├── config/
│       │   └── celery_config.py   # broker/backend, queues, routing, serialization settings
│       └── workers/
│           └── tasks.py           # welcome_email, payment_email task definitions
├── tests/
│   └── __init__.py
├── poetry.lock
├── pyproject.toml
└── README.md
```

Since this is a `src/`-layout Poetry project, commands reference the package as `celery_learning.<module>` (e.g. `celery_learning.celery_app`, `celery_learning.main`) — Poetry's build system resolves `src/celery_learning` as the installed package root, so no path prefix like `src.` is needed in commands.