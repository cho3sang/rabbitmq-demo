import os
import threading
from typing import List

from rabbitmq import DEFAULT_DEAD_LETTER_QUEUE, DEFAULT_QUEUE, RabbitMQ


def make_handler(worker_name: str):
    def handle_message(channel, method, properties, body) -> None:
        print(f"[{worker_name}] received: {body.decode()}")

    return handle_message


def run_worker(
    worker_id: int,
    queue_name: str,
    dead_letter_queue: str,
    clients: List[RabbitMQ],
) -> None:
    client = RabbitMQ()
    clients.append(client)
    worker_name = f"worker-{worker_id}"
    print(f"{worker_name} waiting for messages on '{queue_name}'.")
    client.consume(queue_name, make_handler(worker_name), dead_letter_queue)


def main() -> None:
    queue_name = os.getenv("RABBITMQ_QUEUE", DEFAULT_QUEUE)
    dead_letter_queue = os.getenv("RABBITMQ_DLQ", DEFAULT_DEAD_LETTER_QUEUE)
    worker_count = max(1, int(os.getenv("WORKER_COUNT", "1")))
    clients: List[RabbitMQ] = []
    threads: List[threading.Thread] = []

    try:
        print(f"Starting {worker_count} consumer worker(s). Press Ctrl+C to exit.")
        for worker_id in range(1, worker_count + 1):
            thread = threading.Thread(
                target=run_worker,
                args=(worker_id, queue_name, dead_letter_queue, clients),
            )
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()
    except KeyboardInterrupt:
        print("\nConsumer stopped by user.")
    except Exception as exc:
        print(f"Consumer error: {exc}")
    finally:
        for client in clients:
            client.stop_consuming()
            client.close()


if __name__ == "__main__":
    main()
