from types import SimpleNamespace
from typing import Tuple
import unittest
from unittest.mock import MagicMock, patch

import pika

from rabbitmq import DEFAULT_DEAD_LETTER_QUEUE, RabbitMQ


class FakeChannel:
    def __init__(self) -> None:
        self.queue_declare = MagicMock()
        self.basic_publish = MagicMock()
        self.basic_qos = MagicMock()
        self.basic_consume = MagicMock()
        self.basic_ack = MagicMock()
        self.basic_nack = MagicMock()
        self.start_consuming = MagicMock()
        self.stop_consuming = MagicMock()


class RabbitMQClientTests(unittest.TestCase):
    def make_client(self) -> Tuple[RabbitMQ, FakeChannel]:
        client = RabbitMQ.__new__(RabbitMQ)
        client.connection = SimpleNamespace(is_open=True, add_callback_threadsafe=MagicMock())
        client.channel = FakeChannel()
        return client, client.channel

    def test_declare_queue_configures_dead_letter_queue(self) -> None:
        client, channel = self.make_client()

        client.declare_queue("jobs", DEFAULT_DEAD_LETTER_QUEUE)

        channel.queue_declare.assert_any_call(
            queue=DEFAULT_DEAD_LETTER_QUEUE,
            durable=True,
        )
        channel.queue_declare.assert_any_call(
            queue="jobs",
            durable=True,
            arguments={
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": DEFAULT_DEAD_LETTER_QUEUE,
            },
        )

    def test_publish_uses_persistent_messages(self) -> None:
        client, channel = self.make_client()

        client.publish("jobs", "hello")

        _, kwargs = channel.basic_publish.call_args
        self.assertEqual(kwargs["exchange"], "")
        self.assertEqual(kwargs["routing_key"], "jobs")
        self.assertEqual(kwargs["body"], "hello")
        self.assertIsInstance(kwargs["properties"], pika.BasicProperties)
        self.assertEqual(kwargs["properties"].delivery_mode, 2)

    def test_consume_acks_successful_callback(self) -> None:
        client, channel = self.make_client()
        callback = MagicMock()

        client.consume("jobs", callback)
        wrapped_callback = channel.basic_consume.call_args.kwargs["on_message_callback"]
        method = SimpleNamespace(delivery_tag=123)

        wrapped_callback(channel, method, None, b"hello")

        callback.assert_called_once_with(channel, method, None, b"hello")
        channel.basic_ack.assert_called_once_with(delivery_tag=123)

    def test_consume_nacks_failed_callback_without_requeue_by_default(self) -> None:
        client, channel = self.make_client()

        def failing_callback(channel, method, properties, body):
            raise RuntimeError("processing failed")

        client.consume("jobs", failing_callback)
        wrapped_callback = channel.basic_consume.call_args.kwargs["on_message_callback"]
        method = SimpleNamespace(delivery_tag=456)

        with self.assertRaises(RuntimeError):
            wrapped_callback(channel, method, None, b"bad message")

        channel.basic_nack.assert_called_once_with(delivery_tag=456, requeue=False)

    def test_constructor_retries_connection(self) -> None:
        fake_connection = MagicMock()
        fake_connection.channel.return_value = MagicMock()

        with patch("rabbitmq.time.sleep") as sleep, patch(
            "rabbitmq.pika.BlockingConnection",
            side_effect=[pika.exceptions.AMQPConnectionError(), fake_connection],
        ):
            client = RabbitMQ()

        self.assertIs(client.connection, fake_connection)
        sleep.assert_called_once_with(1.0)

    def test_stop_consuming_uses_threadsafe_callback(self) -> None:
        client, channel = self.make_client()

        client.stop_consuming()

        client.connection.add_callback_threadsafe.assert_called_once_with(channel.stop_consuming)


if __name__ == "__main__":
    unittest.main()
