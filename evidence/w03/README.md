2 file csv là kết quả của file kiểm thử consumer, bao gồm:

- file consumer_error.csv kiểm tra trạng thái đọc các message trong từng partition của topic. Các cột lần lượt là: offset,partition,topic,status.

- file consumer_stats.csv đo lường các thông số của consumer kiểm thử nhận theo batch từ kafka với khoảng thời gian giữa các lần thống kê là 1 ms. Các cột lần lượt là: timestamp,total_received_messages,total_received_bytes.