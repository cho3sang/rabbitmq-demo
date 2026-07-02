import os
import time
from typing import Callable, Optional

import pika


DEFAULT_QUEUE = "test_queue"
DEFAULT_DEAD_LETTER_QUEUE = "test_queue.dlq"


class RabbitMQ:
    def __init__(self) -> None:
        host = os.getenv("RABBITMQ_HOST", "localhost")
        port = int(os.getenv("RABBITMQ_PORT", "5672"))
        user = os.getenv("RABBITMQ_USER", "guest")
        password = os.getenv("RABBITMQ_PASSWORD", "guest")
        heartbeat = int(os.getenv("RABBITMQ_HEARTBEAT", "60"))
        max_retries = int(os.getenv("RABBITMQ_CONNECT_RETRIES", "5"))
        retry_delay = float(os.getenv("RABBITMQ_RETRY_DELAY", "1.0"))

        credentials = pika.PlainCredentials(user, password)
        parameters = pika.ConnectionParameters(
            host=host,
            port=port,
            credentials=credentials,
            heartbeat=heartbeat,
        )

        self.connection = None
        self.channel = None

        for attempt in range(1, max_retries + 1):
            try:
                self.connection = pika.BlockingConnection(parameters)
                self.channel = self.connection.channel()
                return
            except (pika.exceptions.AMQPConnectionError, OSError) as exc:
                if attempt == max_retries:
                    raise ConnectionError(
                        f"Unable to connect to RabbitMQ at {host}:{port} "
                        f"after {max_retries} attempts."
                    ) from exc

                # retry_delay is the base delay; each retry doubles the wait time.
                delay = retry_delay * (2 ** (attempt - 1))
                print(
                    f"RabbitMQ not ready yet (attempt {attempt}/{max_retries}). "
                    f"Retrying in {delay:.1f}s..."
                )
                time.sleep(delay)

    def declare_queue(
        self,
        queue_name: str,
        dead_letter_queue: Optional[str] = DEFAULT_DEAD_LETTER_QUEUE,
    ) -> None:
        arguments = None

        if dead_letter_queue:
            self.channel.queue_declare(queue=dead_letter_queue, durable=True)
            arguments = {
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": dead_letter_queue,
            }

        self.channel.queue_declare(
            queue=queue_name,
            durable=True,
            arguments=arguments,
        )

    def publish(
        self,
        queue_name: str,
        message: str,
        dead_letter_queue: Optional[str] = DEFAULT_DEAD_LETTER_QUEUE,
    ) -> None:
        self.declare_queue(queue_name, dead_letter_queue)
        self.channel.basic_publish(
            exchange="",
            routing_key=queue_name,
            body=message,
            properties=pika.BasicProperties(delivery_mode=2),
        )

    def consume(
        self,
        queue_name: str,
        callback: Callable,
        dead_letter_queue: Optional[str] = DEFAULT_DEAD_LETTER_QUEUE,
        requeue_on_error: bool = False,
    ) -> None:
        self.declare_queue(queue_name, dead_letter_queue)

        def wrapped_callback(channel, method, properties, body):
            try:
                callback(channel, method, properties, body)
                channel.basic_ack(delivery_tag=method.delivery_tag)
            except Exception:
                channel.basic_nack(
                    delivery_tag=method.delivery_tag,
                    requeue=requeue_on_error,
                )
                raise

        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue=queue_name, on_message_callback=wrapped_callback)
        self.channel.start_consuming()

    def stop_consuming(self) -> None:
        if self.connection is not None and self.connection.is_open and self.channel is not None:
            self.connection.add_callback_threadsafe(self.channel.stop_consuming)

    def close(self) -> None:
        if self.connection is not None and self.connection.is_open:
            self.connection.close()
