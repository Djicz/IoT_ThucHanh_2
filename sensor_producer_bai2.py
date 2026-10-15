"""Bài 2 - Mô phỏng nhiều cảm biến gửi telemetry dạng JSON."""

import argparse
import json
import os
import random
import time
from datetime import datetime, timezone

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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Mô phỏng cảm biến IoT")
    parser.add_argument("--devices", type=int, default=3, help="Số cảm biến mô phỏng")
    parser.add_argument("--interval", type=float, default=3.0, help="Chu kỳ gửi (giây)")
    parser.add_argument("--count", type=int, default=0, help="Số chu kỳ; 0 là chạy liên tục")
    return parser.parse_args()


def make_payload(device_number: int) -> dict:
    return {
        "device_id": f"sensor{device_number:02d}",
        "temperature": round(random.uniform(20.0, 40.0), 1),
        "humidity": round(random.uniform(30.0, 80.0), 1),
        "sent_at": datetime.now(timezone.utc).isoformat(),
    }


def main() -> None:
    args = parse_args()
    if args.devices < 1 or args.interval < 0 or args.count < 0:
        raise SystemExit("Tham số phải hợp lệ: devices >= 1, interval >= 0, count >= 0")

    connection = pika.BlockingConnection(connection_parameters())
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    cycle = 0

    print(f"Bat dau gui du lieu cua {args.devices} cam bien. Nhan Ctrl+C de dung.")
    try:
        while args.count == 0 or cycle < args.count:
            cycle += 1
            for device_number in range(1, args.devices + 1):
                payload = make_payload(device_number)
                message = json.dumps(payload, ensure_ascii=False)
                channel.basic_publish(
                    exchange="",
                    routing_key=QUEUE_NAME,
                    body=message.encode("utf-8"),
                    properties=pika.BasicProperties(
                        content_type="application/json",
                        content_encoding="utf-8",
                        delivery_mode=2,
                    ),
                )
                print(f"Da gui: {message}")
            if args.count == 0 or cycle < args.count:
                time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nDa dung sensor producer.")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
