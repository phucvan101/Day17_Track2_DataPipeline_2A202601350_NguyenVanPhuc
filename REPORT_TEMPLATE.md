# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** … **Lớp:** AICB-P2T2 **Ngày:** …

---

## 0 · Kết quả `make verify`

<details>
<summary>Dán nguyên output ba lần chạy vào đây</summary>

```
(dán output make verify)
```

</details>

Tổng kết: **… / 4 tiêu chí đạt**

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

|                    |                                                                          |
| ------------------ | ------------------------------------------------------------------------ |
| **Triệu chứng**    | phình sau mỗi lần chạy                                                   |
| **Nguyên nhân**    | mỗi lần chạy lại là INSERT thêm thay vì merge.                           |
| **Cách khắc phục** | Cần chọn merge theo ticket_id, và sửa catchup/max_active_runs trong DAG. |
| **Bằng chứng**     | trước: 38,750 hàng · sau: 12,480 hàng · checksum 3 lượt:                 |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

|                        |                                                                                                                           |
| ---------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| **Triệu chứng**        | thiếu hàng ở ngày cũ                                                                                                      |
| **P99 độ trễ đo được** | 2.73 ngày (~65 giờ)                                                                                                       |
| **Lookback đã chọn**   | 3 ngày                                                                                                                    |
| **Nguyên nhân**        | so sánh event_date > max(event_date) không có lookback                                                                    |
| **Cách khắc phục**     | Cần đo P99 độ trễ rồi mở lookback window + đổi sang unique_key=['event_date','customer_id'] với delete+insert hoặc merge. |
| **Bằng chứng**         | trước: 8,645 hàng · sau: 9,100 hàng                                                                                       |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> …

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

|                                                        |                                                                                                                                                     |
| ------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Triệu chứng**                                        | đổi kiểu giữa chừng                                                                                                                                 |
| **Nguyên nhân**                                        | nguồn CDC đổi từ số sang nhãn chữ (urgent/high/medium/low) giữa chừng.                                                                              |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** |                                                                                                                                                     |
| **Cách khắc phục**                                     | sửa thứ tự lọc-trước-xếp-hạng-sau trong silver_tickets, điền điều kiện quarantine_tickets, và bật contract + accepted_values test trong schema.yml. |
| **Bằng chứng**                                         | `quarantine_tickets` = … hàng · `dbt test` … pass                                                                                                   |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để
pipeline dừng khi gặp bản ghi lỗi?

> …

---

## 4 · _(mở rộng, không bắt buộc)_ Bài trong EXTRA.md

|                    |                   |
| ------------------ | ----------------- |
| **Bài đã làm**     | A / B / không làm |
| **Nguyên nhân**    |                   |
| **Cách khắc phục** |                   |
| **Bằng chứng**     |                   |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
| -------- | ------------------------------------------------------------------------- |
| 1        |                                                                           |
| 2        |                                                                           |
| 3        |                                                                           |
