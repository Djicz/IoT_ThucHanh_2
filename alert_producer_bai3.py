"""Bài 3 - Phát cảnh báo qua direct exchange theo routing key."""

import argparse
import os

import pika


EXCHANGE_NAME = "iot_alert_exchange"
VALID_LEVELS = ("info", "warning", "critical")


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
    parser = argparse.ArgumentParser(description="Gửi cảnh báo IoT")
    parser.add_argument("level", nargs="?", choices=VALID_LEVELS)
    parser.add_argument("message", nargs="*")
    return parser.parse_args()


def publish(channel, level: str, message: str) -> None:
    channel.basic_publish(
        exchange=EXCHANGE_NAME,
        routing_key=level,
        body=message.encode("utf-8"),
        properties=pika.BasicProperties(
            content_type="text/plain",
            content_encoding="utf-8",
            delivery_mode=2,
        ),
    )
    print(f"Da gui [{level}]: {message}")


def main() -> None:
    args = parse_args()
    connection = pika.BlockingConnection(connection_parameters())
    try:
        channel = connection.channel()
        channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="direct", durable=True)

        if args.level:
            message = " ".join(args.message) or f"Canh bao muc {args.level}"
            publish(channel, args.level, message)
        else:
            examples = {
                "info": "He thong dang hoat dong binh thuong",
                "warning": "Nhiet do phong may vuot nguong warning",
                "critical": "Cam bien kho lanh mat ket noi critical",
            }
            for level, message in examples.items():
                publish(channel, level, message)
    finally:
        connection.close()


if __name__ == "__main__":
    main()
