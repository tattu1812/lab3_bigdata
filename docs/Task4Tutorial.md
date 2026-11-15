# Task 4 — Analytics và controlled layout experiment

> **Big Data Lab 3 — Lakehouse Processing & Analytics**  
> Phạm vi: chạy 3 SQL analytics, tạo 2 biểu đồ, đo layout A/B bằng cùng một query và bàn giao version cho Task 5.

## 1. Điều kiện tiên quyết

Task 3 phải hoàn tất trước khi chạy Task 4. Cần có:

~~~text
evidence/state.json
evidence/incremental.json
evidence/silver_after_batch02.jsonl
evidence/gold_after_batch02.json
~~~

Kiểm tra version binding:

~~~bash
python -c "import json; s=json.load(open('evidence/state.json')); print({k:s.get(k) for k in ['silver_final_version','silver_replay_version','gold_final_version','gold_source_silver_version']})"
~~~

Điều kiện quan trọng:

~~~text
gold_source_silver_version == silver_final_version
~~~

Task 4 phải dùng các giá trị silver_final_version và gold_final_version từ state.json. Không tự dùng version mới nhất nếu version đó là replay version.

## 2. Mục tiêu Task 4

Task 4 gồm hai phần:

1. **Analytics:** đọc Gold snapshot đã pin và tạo ba kết quả SQL cùng hai biểu đồ.
2. **Benchmark:** tạo hai bảng vật lý tương đương, chạy cùng một query sáu lần và so sánh timing.

Benchmark là thí nghiệm trên dữ liệu synthetic. Replica không phải observation độc lập và không được ghi vào operational Gold.

## 3. Implement benchmark_query

Mở đúng file:

~~~text
templates/student_impl.py
~~~

Không sửa file nested trong data/Lab3-Student-Starter/.

Thay phần raise NotImplementedError('TODO 4: benchmark_query') bằng:

~~~python
def benchmark_query(df):
    from pyspark.sql import functions as F

    return (
        df.where(
            F.col("event_date") == F.to_date(F.lit("2026-02-06"))
        )
        .groupBy("site")
        .agg(
            F.count(F.lit(1)).cast("bigint").alias("n_observations"),
            F.sum("temperature_c")
             .cast("decimal(18,2)")
             .alias("sum_temperature_c"),
            F.sum(
                F.when(F.col("is_late"), F.lit(1))
                 .otherwise(F.lit(0))
            )
            .cast("bigint")
            .alias("n_late")
        )
        .select(
            "site",
            "n_observations",
            "sum_temperature_c",
            "n_late"
        )
    )
~~~

Yêu cầu:

- Lọc đúng event_date = DATE '2026-02-06'.
- Group chỉ theo site.
- Trả đúng bốn cột: site, n_observations, sum_temperature_c, n_late.
- Không group theo replica_id.
- Không gọi collect() trong hàm.
- Dùng cùng một biểu thức cho layout A và B.

Kiểm tra syntax:

~~~bash
python -m py_compile templates/student_impl.py
~~~

## 4. Chạy analytics và tạo biểu đồ

~~~bash
python tools/analytics_and_dashboard.py --config config/local.json
~~~

Script đọc Gold tại state['gold_final_version'] và tạo:

| File | Nội dung |
|---|---|
| evidence/Q1_site_day.json | KPI theo site/ngày |
| evidence/Q2_weighted_day.json | Weighted mean và naive mean of means |
| evidence/Q3_above25_rate.json | Tỷ lệ observation trên 25°C |
| evidence/daily_mean.png | Biểu đồ weighted daily mean |
| evidence/late_fraction.png | Biểu đồ late-arrival fraction |

### Q1 — Site/day KPI

Q1 lấy trực tiếp các chỉ số từ Gold. mean_temperature_c là trung bình theo observation, không phải trung bình của các sensor.

### Q2 — Weighted mean

Daily weighted mean phải tính từ additive components:

~~~text
SUM(sum_temperature_c) / SUM(n_observations)
~~~

AVG(mean_temperature_c) là mean không trọng số của các site means. Hai giá trị có thể khác nhau khi số observation giữa các site khác nhau.

### Q3 — Above-25 rate

~~~text
n_above_25 / n_observations
~~~

Ngưỡng là nghiêm ngặt: chỉ tính temperature_c > 25, không tính bằng 25.

## 5. Chạy controlled layout benchmark

~~~bash
python src/run_lab.py \
  --config config/local.json \
  --trusted-manifest data/trusted_lab3_inputs.json \
  --step benchmark
~~~

Runner sẽ:

1. Đọc Silver tại silver_final_version.
2. Tạo 100 replica ID cho mỗi Silver row.
3. Tạo layout A: Delta không partition, ghi với repartition(32).
4. Tạo layout B: Delta partition theo event_date, ghi với năm writer partitions.
5. Chạy một warm-up cho mỗi layout.
6. Đo theo thứ tự A–B, B–A, A–B, tổng cộng sáu lần.
7. So sánh kết quả A/B trước khi chấp nhận timing.
8. Ghi physical plans và benchmark report.

Với Silver sau Task 3 có 9.710 dòng và copy factor 100, benchmark có khoảng 971.000 dòng synthetic. Đây không phải số observation độc lập.

## 6. Evidence cần kiểm tra

Sau khi benchmark thành công:

~~~text
evidence/benchmark_report.json
evidence/plan_A.txt
evidence/plan_B.txt
evidence/Q1_site_day.json
evidence/Q2_weighted_day.json
evidence/Q3_above25_rate.json
evidence/daily_mean.png
evidence/late_fraction.png
~~~

Đọc nhanh benchmark report:

~~~bash
python - <<'PY'
import json
from pathlib import Path

r = json.loads(Path('evidence/benchmark_report.json').read_text())
print('source_silver_version:', r['source_silver_version'])
print('source_rows:', r['source_rows'])
print('expanded_rows:', r['expanded_rows'])
print('median_seconds:', r['median_seconds'])
print('A_median_over_B_median:', r['A_median_over_B_median'])
print('measurements:', len(r['measurements']))
print('query_result_rows:', len(r['query_result']))
PY
~~~

Cần xác nhận:

~~~text
measurements == 6
mọi result_equal == true
source_silver_version == state['silver_final_version']
expanded_rows == source_rows * benchmark_copies
~~~

## 7. Đọc physical plan

Mở:

~~~text
evidence/plan_A.txt
evidence/plan_B.txt
~~~

Trong plan của layout B cần tìm PartitionFilters liên quan đến event_date. Đây là bằng chứng query có thể loại bớt partition theo ngày.

Không được kết luận số file của bảng là số file thực tế đã scan. total_table_data_files chỉ là metadata tổng. Scan metrics thực tế cần lấy từ Spark UI hoặc event log nếu runtime cung cấp.

## 8. Diễn giải timing

Báo cáo cần có:

- đủ sáu timing;
- median của A;
- median của B;
- tỷ lệ median(A) / median(B);
- điều kiện cache là warmed/repeated execution;
- thời gian tạo bảng được tách khỏi query timing.

clearCache() không xóa cache của operating system hoặc object store. Đây không phải cold-cache benchmark.

Không yêu cầu layout B phải nhanh hơn A. Nếu B chậm hơn nhưng kết quả giống hệt, cần giải thích overhead metadata, partition discovery hoặc đặc điểm dữ liệu bounded.

## 9. Lỗi thường gặp

| Lỗi | Cách xử lý |
|---|---|
| BENCHMARK_RESULTS_DIFFER | Kiểm tra query có group theo replica_id hoặc dùng biểu thức khác nhau không |
| TIMED_QUERY_RESULT_DIFFERENCE | Không tiếp tục so sánh timing khi kết quả chưa tương đương |
| Không có state.json | Task 2/Task 3 chưa hoàn thành hoặc dùng sai evidence_dir |
| Gold không đúng version | Kiểm tra gold_source_silver_version và silver_final_version |
| Không thấy PartitionFilters | Kiểm tra layout B có dùng partitionBy('event_date') không |
| Benchmark quá chậm | Tách thời gian setup khỏi query timing |
| Benchmark rows xuất hiện trong Gold | Sai phạm vi; benchmark phải nằm ngoài operational Gold |

Không chạy VACUUM, không xóa _delta_log và không sửa baseline để làm benchmark PASS.

## 10. Checklist hoàn thành Task 4

- [ ] benchmark_query đã implement đúng bốn cột.
- [ ] Không có collect() trong benchmark_query.
- [ ] Python syntax PASS.
- [ ] Q1, Q2, Q3 đã được tạo.
- [ ] Hai biểu đồ đã được tạo.
- [ ] Layout A và B dùng cùng một query.
- [ ] Có một warm-up cho mỗi layout.
- [ ] Có đủ sáu timing.
- [ ] Mọi kết quả A/B đều tương đương.
- [ ] Có plan_A.txt và plan_B.txt.
- [ ] Có median A, median B và tỷ lệ A/B.
- [ ] Báo cáo ghi rõ replica là synthetic.
- [ ] Benchmark không ghi vào operational Gold.
- [ ] source_silver_version khớp với state['silver_final_version'].

## 11. Bàn giao cho Task 5

Task 5 cần:

~~~text
state['silver_final_version']
state['gold_final_version']
state['gold_source_silver_version']
state['silver_path']
state['gold_path']
evidence/benchmark_report.json
evidence/plan_A.txt
evidence/plan_B.txt
~~~

Không tự thay đổi version trong state.json. Version phải là version thực tế do Delta runner ghi nhận.

