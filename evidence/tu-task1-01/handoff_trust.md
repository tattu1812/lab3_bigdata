# TASK 1 – BÁO CÁO XÁC MINH HANDOFF & TRUST

## 1. Kiểm tra tính toàn vẹn dữ liệu (Input Integrity – H01)

Đã thực hiện kiểm tra 16 artifact đầu vào bằng `tools/check_inputs.py` với trusted manifest được cung cấp.

**Kết quả:** 16/16 artifact đều có trạng thái `passed: true`.

SHA-256 của trusted manifest:

`3cdfc84b26d290100b39ce65d4ff7cb22839f4021b9caf26897c8a4ff68091a2`

Kết quả được lưu tại `input_integrity.json`.

## 2. Xác minh nguồn dữ liệu bàn giao (Handoff & Trust – H02)

Nguồn dữ liệu sử dụng là:

`data/handoff/curated.csv`

Thông tin handoff:

- Nguồn: Lab 2
- Định dạng: CSV fallback
- Số bản ghi: 9.610
- Số cột: 10
- Múi giờ: UTC
- Loại handoff: `course_fallback_not_student_approval`

Đây là bộ dữ liệu CSV fallback do khóa học cung cấp, được sử dụng khi chưa có approved Parquet từ Lab 2.

**Lưu ý:** CSV fallback không phải bản Parquet đã được nhóm phê duyệt. Hai định dạng có thể tương đương về dữ liệu logic nhưng không có cùng SHA-256 ở mức file.

Handoff manifest đã khai báo `data_sha256` và `logical_sha256`. Việc tính toán độc lập để xác nhận lại toàn bộ canonical logical digest chưa được thực hiện trong lần kiểm tra này.

## 3. Kiểm tra khả năng truy vết bản ghi (Record-Level Provenance)

Đã thực hiện truy vết bản ghi `R000381` từ dữ liệu curated về dữ liệu nguồn Lab 2.

Thông tin truy vết:

- `record_id`: R000381
- `source_object`: observations_a.csv
- `source_row`: 381
- `sensor_id`: S01
- `temperature_c`: 26.10

SHA-256 của file nguồn:

`5157418915d867980fac8d84551a8b856bba1669ca2e33cef91a15ee82101aea`

**Kết quả xác minh:**

- SHA-256 thực tế của file nguồn khớp với giá trị được khai báo.
- `record_id` và `sensor_id` khớp.
- Thời gian ghi nhận và thời gian tiếp nhận khớp.
- Giá trị nhiệt độ khớp.
- Vị trí bản ghi trong file nguồn khớp.

Như vậy, đã chứng minh được khả năng truy vết một bản ghi từ dữ liệu curated về đúng file nguồn, vị trí và hash.

## 4. Kiểm tra Delta Schema Enforcement (H03)

Đã thực hiện Delta schema probe để kiểm tra khả năng từ chối dữ liệu không phù hợp với schema.

Kết quả:

- `rejected_as_analysis_error`: true
- `rows_after`: 8
- `passed`: true

**Kết luận:** Delta từ chối thao tác ghi sai schema và giữ nguyên 8 bản ghi ban đầu.

## 5. Kiểm tra môi trường thực thi (Runtime & Worker – H04)

Thông tin môi trường:

| Thành phần | Phiên bản / Giá trị |
|---|---|
| Python | 3.11.18 |
| Apache Spark | 4.0.1 |
| Delta Spark | 4.0.1 |
| Java | 17.0.20.1 |
| Hadoop | 3.4.1 |
| Spark Master | local[2] |
| Timezone | UTC |
| Distinct Hosts | 1 |

Spark được thực thi ở chế độ `local[2]`, sử dụng hai luồng thực thi cục bộ, không phải hệ thống Spark cluster đa máy.

Thông tin được ghi nhận tại `runtime_preflight.json` và `worker_probe.json`.

## 6. Kết luận

Task 1 đã thực hiện các nội dung chính:

1. Xác minh tính toàn vẹn của 16 artifact đầu vào.
2. Xác định và ghi nhận nguồn dữ liệu CSV fallback từ Lab 2.
3. Kiểm chứng provenance của một bản ghi cụ thể.
4. Xác nhận Delta Schema Enforcement hoạt động đúng.
5. Ghi nhận môi trường Spark, Delta, Java, Hadoop và thông tin worker.

Các bằng chứng được lưu trong thư mục `evidence/tu-task1-01/`.

**Giới hạn còn lại:** Chưa có bằng chứng tính toán độc lập để xác nhận toàn bộ canonical logical digest. Không tuyên bố CSV fallback là approved Parquet.
