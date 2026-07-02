import os
import sys

from rabbitmq import DEFAULT_DEAD_LETTER_QUEUE, DEFAULT_QUEUE, RabbitMQ


def main() -> None:
    queue_name = os.getenv("RABBITMQ_QUEUE", DEFAULT_QUEUE)
    dead_letter_queue = os.getenv("RABBITMQ_DLQ", DEFAULT_DEAD_LETTER_QUEUE)
    message = " ".join(sys.argv[1:]) or "Hello from the RabbitMQ publisher demo!"

    client = RabbitMQ()
    try:
        client.publish(queue_name, message, dead_letter_queue)
        print(f"Published message to '{queue_name}': {message}")
    finally:
        client.close()


if __name__ == "__main__":
    main()
