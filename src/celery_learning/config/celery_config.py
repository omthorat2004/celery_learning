from kombu import Queue, Exchange

broker_url="redis://localhost:6379/0" # message queue
result_backend="redis://localhost:6379/1" # response is stored after task is executed 

task_serializer="json" # We need to serialise the task detail becuase redis can only store bytes or string
result_serializer="json" # same here also we need to serialize due to broker data storing type format
accept_content = ["json"]  # The Celery worker will only accept incoming messages serialized as JSON

timezone="Asia/Kolkata" # timezone which is used by the Celery Beat for scheduled tasks
enable_utc=True # # Store and process times internally in UTC, converting
# to the configured timezone when needed.

include=[
    "celery_learning.workers.tasks" # Celery should know that tasks are imported if you dont need these you can directly import task in celery_app.py
]

task_track_started = True # tells celery to update the task status to STARTED when a worker begins a task
result_expires=3600 # results expires after one hour

# Declare queues explicitly
task_queues = (
    Queue("high", Exchange("high"), routing_key="high"),  # Read about Exchange and routing key in detail form other resources
    Queue("low", Exchange("low"), routing_key="low"),
)

task_default_queue = "low"  # fallback queue if a task isn't routed anywhere


task_routes={
    "celery_learning.workers.tasks.payment_email": {"queue": "high"},
    "celery_learning.workers.tasks.welcome_email": {"queue": "low"},
}