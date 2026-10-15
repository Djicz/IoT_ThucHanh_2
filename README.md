# Thực hành lập trình Python với AMQP

Bộ bài thực hành gồm ba bài: gửi nhận message qua queue, mô phỏng cảm biến IoT và điều phối cảnh báo qua direct exchange.

## Broker sử dụng

Mặc định các chương trình kết nối RabbitMQ tại `localhost:5672`, virtual host `/`, tài khoản `guest` và mật khẩu `guest`.

Có thể thay đổi cấu hình bằng các biến môi trường:

- `AMQP_HOST`: địa chỉ broker
- `AMQP_PORT`: cổng AMQP
- `AMQP_VHOST`: virtual host
- `AMQP_USER`: tên đăng nhập
- `AMQP_PASSWORD`: mật khẩu

Ví dụ PowerShell khi dùng broker khác:

```powershell
$env:AMQP_HOST = "192.168.1.10"
$env:AMQP_USER = "student"
$env:AMQP_PASSWORD = "secret"
```

## Chuẩn bị

1. Cài Python 3 và khởi động RabbitMQ.
2. Mở terminal tại thư mục này.
3. Cài thư viện:

```powershell
pip install pika
```

## Cách chạy từng chương trình

### Bài 1 Gửi và nhận message cơ bản

Mở terminal thứ nhất và chạy consumer:

```powershell
python consumer_bai1.py
```

Mở terminal thứ hai và chạy producer. Thay họ tên và mã sinh viên bằng thông tin thật:

```powershell
python producer_bai1.py --name "Nguyen Van A" --student-id "B21xxxx"
```

Gửi nhiều message liên tiếp:

```powershell
python producer_bai1.py --name "Nguyen Van A" --student-id "B21xxxx" --count 5 --interval 1
```

Kết quả: producer đưa message vào `iot_lab_queue`; consumer in nội dung message và thời điểm nhận. Consumer chạy liên tục cho đến khi nhấn `Ctrl+C`.

### Bài 2 Mô phỏng cảm biến IoT

Mở terminal thứ nhất:

```powershell
python monitor_consumer_bai2.py
```

Mở terminal thứ hai:

```powershell
python sensor_producer_bai2.py
```

Producer mặc định mô phỏng ba cảm biến, gửi dữ liệu JSON mỗi 3 giây và bổ sung thời gian gửi theo UTC. Có thể chạy đúng một cảm biến trong năm chu kỳ:

```powershell
python sensor_producer_bai2.py --devices 1 --count 5 --interval 3
```

Kết quả: dữ liệu được gửi vào `sensor_data_queue`. Consumer hiển thị mã thiết bị, nhiệt độ, độ ẩm và phát cảnh báo khi nhiệt độ lớn hơn 35 độ C hoặc độ ẩm nhỏ hơn 40 phần trăm.

### Bài 3 Điều phối cảnh báo với exchange

Mở hai terminal để chạy hai consumer:

```powershell
python warning_consumer_bai3.py
```

```powershell
python critical_consumer_bai3.py
```

Trong terminal thứ ba, chạy producer không có tham số để gửi lần lượt ba mức cảnh báo:

```powershell
python alert_producer_bai3.py
```

Hoặc gửi một cảnh báo tùy chọn:

```powershell
python alert_producer_bai3.py warning Nhiet do phong may vuot nguong warning
python alert_producer_bai3.py critical Cam bien kho lanh mat ket noi critical
```

Kết quả mong đợi:

- Message `info` không được hai consumer trên nhận.
- Message `warning` chỉ được `warning_queue` nhận.
- Message `critical` chỉ được `critical_queue` nhận.
- Cảnh báo nhận được đồng thời được ghi vào `warning_alerts.log` hoặc `critical_alerts.log`.

## Kết quả đạt được

- Bài 1 triển khai đúng luồng `producer -> iot_lab_queue -> consumer`, hỗ trợ gửi một hoặc nhiều message và hiển thị thời điểm nhận.
- Bài 2 gửi payload JSON tuần hoàn từ một hoặc nhiều cảm biến, phân tích dữ liệu và phát hiện đủ hai điều kiện cảnh báo.
- Bài 3 dùng direct exchange và routing key để tách chính xác cảnh báo `warning` và `critical`; message `info` không đi vào hai queue này.
- Consumer dùng xác nhận thủ công sau khi xử lý, queue/exchange được khai báo bền vững và message được đánh dấu persistent.

## Danh sách tệp nộp

- `producer_bai1.py`
- `consumer_bai1.py`
- `sensor_producer_bai2.py`
- `monitor_consumer_bai2.py`
- `alert_producer_bai3.py`
- `warning_consumer_bai3.py`
- `critical_consumer_bai3.py`
- `README.md`

## Lưu ý xử lý lỗi

Nếu bị lỗi kết nối, kiểm tra RabbitMQ đang chạy, cổng 5672 không bị chặn và thông tin trong các biến môi trường là chính xác. Tài khoản `guest` của RabbitMQ thường chỉ được phép đăng nhập từ máy cục bộ.
