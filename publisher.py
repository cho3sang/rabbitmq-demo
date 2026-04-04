from rabbitmq import RabbitMQ


def main() -> None:
    client = RabbitMQ()
    try:
        message = "Hello from the RabbitMQ publisher demo!"
        client.publish("test_queue", message)
        print(f"Published message to 'test_queue': {message}")
    finally:
        client.close()


if __name__ == "__main__":
    main()
