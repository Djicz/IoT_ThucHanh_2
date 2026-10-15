"""Bài 1 - Gửi thông điệp cơ bản vào RabbitMQ."""

import argparse
import os
import time

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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Gửi message vào iot_lab_queue")
    parser.add_argument("--name", default=os.getenv("STUDENT_NAME", "Nguyen Van A"))
    parser.add_argument("--student-id", default=os.getenv("STUDENT_ID", "B21xxxx"))
    parser.add_argument("--count", type=int, default=1, help="Số message cần gửi")
    parser.add_argument("--interval", type=float, default=1.0, help="Khoảng cách giữa các message")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.count < 1:
        raise SystemExit("--count phải lớn hơn hoặc bằng 1")

    connection = pika.BlockingConnection(connection_parameters())
    try:
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE_NAME, durable=True)

        for index in range(1, args.count + 1):
            message = f"Xin chao tu ung dung Python AMQP - {args.student_id} - {args.name}"
            if args.count > 1:
                message += f" - message {index}/{args.count}"
            channel.basic_publish(
                exchange="",
                routing_key=QUEUE_NAME,
                body=message.encode("utf-8"),
                properties=pika.BasicProperties(
                    content_type="text/plain",
                    content_encoding="utf-8",
                    delivery_mode=2,
                ),
            )
            print(f"Da gui: {message}")
            if index < args.count:
                time.sleep(args.interval)
    finally:
        connection.close()


if __name__ == "__main__":
    main()
