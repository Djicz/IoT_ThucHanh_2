"""Bài 3 - Chỉ nhận cảnh báo có routing key critical."""

import logging
import os

import pika


EXCHANGE_NAME = "iot_alert_exchange"
QUEUE_NAME = "critical_queue"
ROUTING_KEY = "critical"
logging.basicConfig(
    filename="critical_alerts.log",
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    encoding="utf-8",
)


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
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="direct", durable=True)
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.queue_bind(exchange=EXCHANGE_NAME, queue=QUEUE_NAME, routing_key=ROUTING_KEY)
    channel.basic_qos(prefetch_count=1)

    def handle_message(ch, method, _properties, body: bytes) -> None:
        message = body.decode("utf-8", errors="replace")
        output = f"[{QUEUE_NAME}] Da nhan: {message}"
        print(output)
        logging.info(output)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=handle_message)
    print(f"Dang lang nghe routing key '{ROUTING_KEY}'. Nhan Ctrl+C de dung.")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\nDa dung critical consumer.")
        channel.stop_consuming()
    finally:
        connection.close()


if __name__ == "__main__":
    main()
