# Distributed Messaging Demo with RabbitMQ

A small distributed-systems project that shows how two services can communicate asynchronously through a message broker. The demo uses RabbitMQ in Docker Compose, a Python publisher that sends work to a queue, and a Python consumer that receives and acknowledges messages.

This project is meant to demonstrate backend and systems fundamentals clearly:

- asynchronous service-to-service communication
- durable queues and persistent messages
- consumer acknowledgements and requeue behavior
- dead-letter queue handling for failed messages
- competing consumer workers for parallel processing
- connection retry with exponential backoff
- unit tests and GitHub Actions CI
- containerized local infrastructure with RabbitMQ's management UI

## Tech Stack

- Python 3.8+
- RabbitMQ
- Docker Compose
- `pika`

## Why This Project Matters

Many student projects stop at REST APIs and database CRUD. This demo focuses instead on message-driven communication, which is common in backend systems that need to decouple producers from consumers and handle work asynchronously.

The implementation includes a few patterns that are useful beyond a toy example:

- durable queues plus persistent messages for safer delivery
- `basic_ack` and `basic_nack` handling in the consumer
- failed messages can be routed to `test_queue.dlq`
- `basic_qos(prefetch_count=1)` for fair task dispatch
- `WORKER_COUNT` support for running multiple competing consumers
- retry and heartbeat settings to make startup and long-running consumers more reliable

## Project Structure

- `docker-compose.yml`: runs RabbitMQ with the management plugin enabled
- `rabbitmq.py`: shared RabbitMQ client wrapper for connecting, publishing, and consuming
- `publisher.py`: sends a sample message to `test_queue`
- `consumer.py`: starts one or more workers that consume from `test_queue`
- `tests/`: unit tests for queue declaration, publishing, consuming, retries, and shutdown
- `requirements.txt`: Python dependency list

## How It Works

1. RabbitMQ runs locally in Docker and exposes AMQP on port `5672`.
2. `publisher.py` sends a message to `test_queue`.
3. `consumer.py` listens to that queue and processes messages asynchronously.
4. The consumer acknowledges successful messages so RabbitMQ can remove them from the queue.
5. If processing fails, the message is negatively acknowledged and routed to the dead-letter queue.

## Prerequisites

- Python 3.8 or newer
- Docker Desktop or Docker Engine with Docker Compose

## Local Setup

1. Create a virtual environment:

   ```bash
   python3 -m venv .venv
   ```

2. Activate it:

   ```bash
   source .venv/bin/activate
   ```

3. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

4. Start RabbitMQ:

   ```bash
   docker compose up -d
   ```

   The Compose service includes a healthcheck, and the Python client retries connections briefly while RabbitMQ finishes starting.

5. Start the consumer in one terminal:

   ```bash
   python3 consumer.py
   ```

6. To demonstrate competing consumers, start multiple workers:

   ```bash
   WORKER_COUNT=3 python3 consumer.py
   ```

7. Publish a message from another terminal:

   ```bash
   python3 publisher.py
   ```

   You can also publish a custom message:

   ```bash
   python3 publisher.py "process this background job"
   ```

8. The consumer should print the received message.

## Tests

Run the unit tests locally:

```bash
python3 -m unittest discover -s tests -v
```

The repository also includes a GitHub Actions workflow that compiles the Python files and runs the test suite on every push and pull request to `main`.

## RabbitMQ Management UI

RabbitMQ's management dashboard is available at [http://localhost:15672](http://localhost:15672).

- Username: `guest`
- Password: `guest`

## Environment Variables

The client reads connection settings from environment variables and falls back to local defaults:

- `RABBITMQ_HOST` default `localhost`
- `RABBITMQ_PORT` default `5672`
- `RABBITMQ_USER` default `guest`
- `RABBITMQ_PASSWORD` default `guest`
- `RABBITMQ_HEARTBEAT` default `60`
- `RABBITMQ_CONNECT_RETRIES` default `5`
- `RABBITMQ_RETRY_DELAY` default `1.0` as the base delay before exponential backoff
- `RABBITMQ_QUEUE` default `test_queue`
- `RABBITMQ_DLQ` default `test_queue.dlq`
- `WORKER_COUNT` default `1`

## Notes and Limitations

- If the consumer callback raises an exception, the message is negatively acknowledged without requeueing so RabbitMQ can route it to the dead-letter queue.
- In a production system, this would usually be paired with retry limits, observability, and replay tooling.
- If RabbitMQ reports that `test_queue` already exists with different arguments, reset the local broker with `docker compose down` and then `docker compose up -d`.
- This demo is intentionally small and focused on message broker fundamentals rather than a full multi-service application.
- The project is free to run locally and does not require paid cloud resources.
