# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Nguyễn Văn Phúc **Lớp:** E403 **Ngày:** 17/08/2026

---

## 0 · Kết quả `make verify`

<details>
<summary>Dán nguyên output ba lần chạy vào đây</summary>

```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 21.6s
  run 2/3 … 21.5s
  run 3/3 … 21.4s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    f8d3f591f0    f8d3f591f0    f8d3f591f0   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  bài mở rộng (EXTRA.md)                      — chưa chạy `make seed-extra`
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  4/4 tiêu chí đạt
```

</details>

Tổng kết: **4 / 4 tiêu chí đạt**

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

|                    |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| ------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Triệu chứng**    | `gold_training_set` phình sau mỗi lần chạy lại pipeline (13,790 → 26,270 hàng chỉ sau 2 lượt), có ticket xuất hiện tới 3 lần.                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| **Root cause**     | `config()` của model khai `materialized='incremental'` nhưng thiếu `unique_key`, nên dbt-duckdb sinh câu `INSERT INTO ... SELECT` thuần (append), không ghi đè. `silver_tickets` chỉ giữ bản ghi mới nhất/ticket, nên ticket tạo ngày D1 rồi sửa ngày D2 sẽ lọt qua điều kiện lọc `_ingested_at` ở **cả hai** lượt chạy D1 và D2 — mỗi lượt insert thêm một bản ghi của cùng ticket vào hai partition ngày khác nhau. Grain của bảng là _entity_ (ticket), nên chiến lược `delete+insert` theo partition ngày không xử lý được trường hợp này; chỉ `merge` theo khoá tự nhiên mới đúng. |
| **Cách khắc phục** | `dbt/models/gold/gold_training_set.sql`: thêm `unique_key='ticket_id'`, `incremental_strategy='merge'`. `dags/ai_training_pipeline.py`: `catchup=False` (tránh Airflow tự backfill chồng lấp khi bấm Clear Task), `max_active_runs=1` (tránh hai run ghi đồng thời vào cùng bảng) — hai tham số này chỉ **giảm tần suất kích hoạt** lỗi, không phải root cause; nếu chỉ sửa DAG mà không sửa `config()` thì `make verify` vẫn đỏ.                                                                                                                                                       |
| **Bằng chứng**     | Trước: 38,750 hàng (kỳ vọng 12,480, thừa 26,270). Sau: **12,480 / 12,480** hàng, ổn định qua 3 lượt chạy — checksum `8dd7c98653` giống hệt nhau ở cả 3 lượt.                                                                                                                                                                                                                                                                                                                                                                                                                            |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

|                        |                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| ---------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Triệu chứng**        | `gold_feature_daily` thiếu ~5% hàng, chỉ ở những ngày đã chạy xong từ lâu; ngày mới thì đủ.                                                                                                                                                                                                                                                                                                                                              |
| **P99 độ trễ đo được** | **2.73 ngày** (~65 giờ). Max = 2.94 ngày. Phân bố có hai cụm tách rời rõ rệt: cụm đúng giờ (0–6 giờ, chiếm phần lớn) và cụm tới muộn (43–71 giờ, ~5.05% tổng số bản ghi) — không có gì ở khoảng giữa.                                                                                                                                                                                                                                    |
| **Lookback đã chọn**   | **3 ngày** — làm tròn lên từ P99 (2.73 ngày), đủ phủ luôn cả max quan sát được (2.94 ngày).                                                                                                                                                                                                                                                                                                                                              |
| **Root cause**         | Điều kiện `is_incremental()` là `event_date > (select max(event_date) from {{ this }})`, không có lookback. Một event có `event_date` D-2 nhưng `_ingested_at` = D chỉ lọt qua filter nếu D-2 lớn hơn `max(event_date)` hiện có trong target — nhưng tại thời điểm đó target đã tiến tới D-1 rồi (từ các lượt chạy trước), nên D-2 **không bao giờ** thoả điều kiện nữa. Đây không phải độ trễ tạm thời mà là **mất dữ liệu vĩnh viễn**. |
| **Cách khắc phục**     | Mở lookback: `where event_date >= (select max(event_date) from {{ this }}) - interval 3 day`. Đổi `unique_key = ['event_date', 'customer_id']` (grain 2 cột) với `incremental_strategy='merge'` — vì window rộng hơn khiến cùng một cặp (ngày, customer) được tính lại ở nhiều lượt chạy, cần merge để lượt sau **thay thế** lượt trước thay vì cộng dồn (nếu không sẽ tái tạo đúng lỗi của Nhiệm vụ 1 trên bảng này).                   |
| **Bằng chứng**         | Trước: 8,645 hàng (kỳ vọng 9,100, thiếu 455). Sau: **9,100 / 9,100** hàng, ổn định qua 3 lượt — checksum `f8d3f591f0` giống hệt nhau, `gold_training_set` không bị ảnh hưởng (vẫn 12,480).                                                                                                                                                                                                                                               |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> Chọn `max` để lookback nghĩa là mọi lượt chạy sau này đều phải quét lại đủ xa để bắt trọn 100% outlier — kể cả những bản ghi cực hiếm chỉ xuất hiện một lần, và giá trị `max` có thể tăng bất ngờ nếu tương lai xuất hiện một bản ghi trễ hơn nữa (dữ liệu sản xuất không có giới hạn cứng). `max` không phải một đại lượng ổn định để thiết kế cấu hình lâu dài. P99 là điểm cân bằng: nắm bắt 99% khối lượng dữ liệu thực tế, chi phí quét lại (số ngày lookback, tốn ở **mọi** lượt chạy về sau) được giữ nhỏ và có thể dự đoán. Trong bộ dữ liệu này P99 (2.73) và max (2.94) rất gần nhau nên làm tròn lên 3 ngày tình cờ phủ được 100%, nhưng nguyên tắc chọn lookback vẫn phải dựa trên percentile đo được (P99), không hard-code theo giá trị max quan sát một lần.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

|                                                            |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| ---------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Triệu chứng**                                            | Từ 08-10, `silver_tickets.priority` có tỷ lệ NULL rất lớn và xuất hiện các giá trị ngoài miền (`0`, `5`, `-1`) dù contract quy định `1..4`. Model phân loại dự đoán kém hẳn từ thời điểm đó.                                                                                                                                                                                                                                                                                                                                                                                                                          |
| **Root cause**                                             | Team backend đổi cách biểu diễn `priority_raw` từ số sang nhãn chữ (`urgent/high/medium/low`) — bản chất là **schema evolution** (đổi format, không đổi ý nghĩa), nhưng macro cũ dùng `try_cast(priority_raw as integer)` nên coi nhãn chữ là lỗi (trả về NULL), đồng thời lại **chấp nhận** các số ngoài miền hợp lệ (`0`, `5`, `-1`) vì chúng cast integer được — sai theo cả hai hướng cùng lúc. `contract.enforced=false` khiến sai kiểu không bị chặn, và không có test miền giá trị nên số rác lọt thẳng vào Silver.                                                                                            |
| **Ba nhóm giá trị `priority_raw` và cách xử lý từng nhóm** | Nhóm 1 — số hợp lệ `1,2,3,4` (đúng contract cũ) → **giữ nguyên**. Nhóm 2 — nhãn chữ `urgent/high/medium/low` (source đổi format, ý nghĩa không đổi) → **map** về `1/2/3/4` theo tài liệu API team backend. Nhóm 3 — rác thật `P1, P2, unknown, 0, 5, -1, '', NULL` → **quarantine** (macro trả `NULL`). Tổng nhóm 3 đo được đúng **312** bản ghi, khớp `expected/quarantine_tickets.count`.                                                                                                                                                                                                                           |
| **Cách khắc phục**                                         | `dbt/macros/normalize_priority.sql`: viết `CASE` xử lý đủ 3 nhóm. `dbt/models/silver/silver_tickets.sql`: tách CTE lọc `normalize_priority(...) is not null` **trước** khi `row_number()` xếp hạng — để ticket có bản ghi mới nhất hỏng vẫn giữ được trạng thái hợp lệ từ lần cập nhật trước, không bị mất cả ticket. `dbt/models/silver/quarantine_tickets.sql`: `where normalize_priority(priority_raw) is null`, dùng chung macro với Silver nên hai bảng không thể lệch nhau. `dbt/models/silver/schema.yml`: bật `contract.enforced: true` + test `not_null` và `accepted_values: [1,2,3,4]` cho cột `priority`. |
| **Bằng chứng**                                             | `quarantine_tickets` = **312 / 312** hàng, ổn định qua 3 lượt. `dbt test` = **11/11 pass** (bản gốc 9 test). `silver_tickets.priority ∈ 1..4, không NULL`: sạch. `gold_training_set` giữ nguyên 12,480 (không bị ảnh hưởng bởi việc loại bớt 312 bản ghi CDC hỏng, vì các ticket đó vẫn còn phiên bản hợp lệ trước đó).                                                                                                                                                                                                                                                                                               |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để pipeline dừng khi gặp bản ghi lỗi?

> Nên chặn ở **Silver**, không phải Bronze. Bronze phải giữ nguyên dữ liệu thô — nếu Bronze từ chối row lỗi ngay từ đầu, ta mất luôn bằng chứng gốc để điều tra sự cố về sau (không còn cách nào biết source thực sự đã gửi gì, khó phân biệt "lỗi ở nguồn" với "lỗi ở transform"). Silver là tầng áp dụng business rule/contract, nên đó mới là nơi đúng để phân loại và tách bản ghi lỗi.
>
> Không nên để `dbt test` fail rồi dừng cả DAG, vì quy mô lỗi rất nhỏ: 312 bản ghi CDC hỏng so với hơn 12,000 ticket hợp lệ, hơn 130,000 event và 31,200 chunk hoàn toàn bình thường. Dừng toàn bộ pipeline vì 312 bản ghi nghĩa là chặn luôn RAG index, classifier và routing agent phục vụ phần dữ liệu tốt tuyệt đối đa số — thiệt hại lớn hơn nhiều so với lợi ích. Cách đúng là tách riêng (quarantine) để pipeline tiếp tục chạy, đồng thời tạo hàng đợi cụ thể cho người trực xử lý — đúng tinh thần "định tuyến bản ghi lỗi thay vì để pipeline dừng".

---

## 4 · _(mở rộng, không bắt buộc)_ Bài trong EXTRA.md

|                    |           |
| ------------------ | --------- |
| **Bài đã làm**     | Không làm |
| **Nguyên nhân**    | —         |
| **Cách khắc phục** | —         |
| **Bằng chứng**     | —         |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên                                                                                                                                                                                     |
| -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1        | Đọc `config()` của mọi incremental model xem có khai `unique_key`/`incremental_strategy` chưa, rồi chạy pipeline hai lần liên tiếp và đếm hàng — đừng tin số liệu nào trước khi xác nhận tính idempotent.                                                     |
| 2        | Đo phân bố độ trễ giữa thời điểm sự kiện xảy ra và thời điểm nó tới kho (`_ingested_at - event_time`) trước khi tin bất kỳ điều kiện lọc incremental nào theo mốc thời gian — đừng giả định dữ liệu luôn tới đúng ngày nó xảy ra.                             |
| 3        | Đối chiếu phân bố giá trị thực tế của cột (`group by ... count(*)`) với data contract đã khai báo, đặc biệt ngay sau khi biết có thông báo thay đổi từ team nguồn — và luôn phân biệt "đổi format" với "dữ liệu lỗi" trước khi quyết định map hay quarantine. |
