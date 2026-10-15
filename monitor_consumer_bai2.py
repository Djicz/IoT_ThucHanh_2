"""Bài 2 - Nhận, phân tích và cảnh báo dữ liệu cảm biến."""

import json
import os

import pika


QUEUE_NAME = "sensor_data_queue"


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
            data = json.loads(body.decode("utf-8"))
            device_id = str(data["device_id"])
            temperature = float(data["temperature"])
            humidity = float(data["humidity"])

            print("-" * 40)
            print(f"Device: {device_id}")
            print(f"Temperature: {temperature:.1f}")
            print(f"Humidity: {humidity:.1f}")
            if "sent_at" in data:
                print(f"Sent at: {data['sent_at']}")
            if temperature > 35:
                print("CANH BAO: Nhiet do cao")
            if humidity < 40:
                print("CANH BAO: Do am thap")
        except (UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
            print(f"Du lieu khong hop le, bo qua message: {exc}")
        finally:
            ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=handle_message)
    print(f"Dang giam sat queue '{QUEUE_NAME}'. Nhan Ctrl+C de dung.")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\nDa dung monitoring consumer.")
        channel.stop_consuming()
    finally:
        connection.close()


if __name__ == "__main__":
    main()
