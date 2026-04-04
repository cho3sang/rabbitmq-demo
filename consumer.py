from rabbitmq import RabbitMQ


def handle_message(channel, method, properties, body) -> None:
    print(f"Received message from 'test_queue': {body.decode()}")


def main() -> None:
    client = None
    try:
        client = RabbitMQ()
        print("Waiting for messages on 'test_queue'. Press Ctrl+C to exit.")
        client.consume("test_queue", handle_message)
    except KeyboardInterrupt:
        print("\nConsumer stopped by user.")
    except Exception as exc:
        print(f"Consumer error: {exc}")
    finally:
        if client is not None:
            client.close()


if __name__ == "__main__":
    main()
