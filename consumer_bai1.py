"""Bài 1 - Nhận thông điệp cơ bản từ RabbitMQ."""

import os
from datetime import datetime

import pika


QUEUE_NAME = "iot_lab_queue"


def connection_parameters() -> pika.ConnectionParameters:
    credentials = pika.PlainCredentials(
        os.getenv("AMQP_USER", "guest"),
        os.getenv("AMQP_PASSWORD", "guest"),
    )
    return pika.ConnectionParameters(
        host=os.getenv("AMQP_HOST", "localhost"),
        port=int(os.getenv("AMQP_PORT", "5672")),
        virtual_host=os.getenv("AMQP_VHOST", "/"),
        credentials=credentials,
        heartbeat=60,
        blocked_connection_timeout=30,
    )


def main() -> None:
    connection = pika.BlockingConnection(connection_parameters())
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.basic_qos(prefetch_count=1)

    def handle_message(ch, method, _properties, body: bytes) -> None:
        try:
            message = body.decode("utf-8")
            print(f"Da nhan message: {message}")
            print(f"Thoi gian nhan: {datetime.now().strftime('%H:%M:%S')}")
        except UnicodeDecodeError as exc:
            print(f"Message khong phai UTF-8: {exc}")
        finally:
            ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=handle_message)
    print(f"Dang lang nghe queue '{QUEUE_NAME}'. Nhan Ctrl+C de dung.")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\nDa dung consumer.")
        channel.stop_consuming()
    finally:
        connection.close()


if __name__ == "__main__":
    main()
