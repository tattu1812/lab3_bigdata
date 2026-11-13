
# Task 3 — Revision, Delta MERGE & Replay

> **Big Data Lab 3 — Lakehouse Processing & Analytics**<br>
> **Phạm vi:** Chọn revision thắng, cập nhật Silver bằng Delta MERGE, chạy incremental và kiểm chứng M01–M08.

**Điều kiện tiên quyết:** Task 1 đã kiểm tra đầu vào/runtime thành công; Task 2 đã bootstrap Bronze, Silver và Gold, tạo `state.json` cho cùng một lần chạy.

Tài liệu này chỉ hướng dẫn **Task 3**, không thay thế các bước thiết lập hoặc bootstrap của Task 1–2.

**Nguyên tắc an toàn:** Các lệnh đọc và unit test được thực hiện trước. Lệnh `--step incremental` **ghi dữ liệu Delta**, vì vậy chỉ chạy khi đã xác minh đúng config, state, bảng đích và trạng thái chưa xử lý batch.

Không xóa `_delta_log`, không `VACUUM` và không chạy lại mù quáng khi gặp lỗi.

---

## 1. Tổng quan cơ chế

```text
Bronze/Silver/Gold đã bootstrap (Task 2)
                |
                v
      Batch revision mới (140 dòng)
                |
                +--> Bronze: ghi receipt vật lý không trùng
                |
                v
         choose_revisions
      Chọn 1 winner / record_id
                |
                v
           merge_silver
        INSERT / UPDATE / NO-OP
                |
                v
       Silver sau cập nhật
                |
                v
       Tính lại Gold từ
       Silver version được pin
                |
                v
    Replay, Time Travel, M01–M08
```

- **Bronze:** lưu các receipt vật lý theo danh tính nguồn, bao gồm cả revision không thắng.
- **Silver:** giữ một bản hiện hành cho mỗi `record_id`.
- **Gold:** thống kê được tính từ một phiên bản Silver xác định.

Với batch mẫu của lab: 140 revision vật lý chứa 130 `record_id` duy nhất; 10 `record_id` có hai revision.

## 2. Hai hàm cần triển khai

### 2.1. `choose_revisions`

**Mục tiêu:** Chỉ chọn một bản thắng trong batch cho mỗi `record_id`.

Thứ tự ưu tiên (so từ trái sang phải):

1. `ingest_time_utc` **DESC** — thời điểm nhận mới hơn thắng.
2. `source_object` **ASC** — phá hòa theo tên nguồn nhỏ hơn.
3. `source_row` **ASC** — phá hòa tiếp theo theo số dòng nhỏ hơn.

Cách thực hiện bằng PySpark:

```python
from pyspark.sql import Window, functions as F

window = Window.partitionBy("record_id").orderBy(
    F.col("ingest_time_utc").desc(),
    F.col("source_object").asc(),
    F.col("source_row").asc(),
)

# Minh họa thuật toán; dùng đúng chữ ký hàm trong starter.
winners = (
    revisions.withColumn("_rank", F.row_number().over(window))
    .filter(F.col("_rank") == 1)
    .drop("_rank")
)
```

**Yêu cầu:** Giữ nguyên schema và provenance (`source_object`, `source_row`, `source_sha256`).

Nếu hai dòng cùng `record_id` có cả ba khóa ưu tiên giống nhau và không thể xác định một thứ tự duy nhất, phải phát hiện lỗi `AMBIGUOUS_REVISION_ORDER`; không tự chọn theo thứ tự đọc file hoặc thêm khóa ưu tiên ngoài hợp đồng.

### 2.2. `merge_silver`

**Mục tiêu:** So sánh từng winner với dòng hiện có trong Silver theo `record_id` bằng **Delta MERGE**.

| Trường hợp | Xử lý | Thay đổi số dòng Silver |
|---|---|---:|
| `record_id` chưa tồn tại | **INSERT** | +1 |
| Incoming có ưu tiên cao hơn | **UPDATE** | 0 |
| Incoming cũ hơn hoặc tương đương | **NO-OP** | 0 |

Chỉ UPDATE khi thỏa **ít nhất một** điều kiện:

```text
source.ingest_time_utc > target.ingest_time_utc
OR
(source.ingest_time_utc = target.ingest_time_utc
 AND source.source_object < target.source_object)
OR
(source.ingest_time_utc = target.ingest_time_utc
 AND source.source_object = target.source_object
 AND source.source_row < target.source_row)
```

**Yêu cầu:** Không UPDATE vô điều kiện, không tạo `record_id` trùng, giữ đúng provenance, `temperature_c` kiểu `DECIMAL(8,2)` và ngày/giờ theo UTC.

Runner kiểm tra tính hợp lệ và provenance của batch trước khi ghi Delta. Riêng kiểm tra thứ tự revision mơ hồ được thực hiện khi chọn winner **sau** bước ghi Bronze; nếu bước này thất bại, Bronze có thể đã thay đổi. Các bảng không có giao dịch nguyên tử liên bảng: cần kiểm tra history và state trước khi xử lý lỗi.

---

## 3. Bước 0 — Kiểm tra điều kiện chạy

Đứng tại thư mục dự án chứa `src/`, `templates/`, `config/`, `tests/`. Dùng lại môi trường Python/Java và cấu hình đã chạy thành công Task 1–2.

```bash
pwd
ls src/run_lab.py templates/student_impl.py tools/check_inputs.py
ls tests/test_choose_revisions.py tests/test_merge_silver.py

python --version
java -version
python -c 'import pyspark; print(pyspark.__version__)'
python -c 'import importlib.metadata as m; print(m.version("delta-spark"))'
```

Runtime tham chiếu: Python 3.11, Java 17 hoặc 21, PySpark 4.0.1, Delta-Spark 4.0.1. Nếu dùng môi trường ảo, cần kích hoạt trước khi chạy các lệnh trên.

Đặt biến theo **cấu hình thật của lần bootstrap**:

```bash
export CFG="/path/to/bootstrap-config.json"             # config đã dùng ở Task 2
export TRUSTED="/path/to/trusted_lab3_inputs.json"      # manifest độc lập do TA cấp

# TABLES là thư mục chứa bronze/, silver/, gold/ của cùng run.
# Lấy đường dẫn thật từ config, state.json và logic path trong runner.
export TABLES="/path/to/lakehouse/group/run/tables"

test -f "$CFG" && test -f "$TRUSTED" || exit 1
for t in bronze silver gold; do
  test -d "$TABLES/$t/_delta_log" || { echo "MISSING_TABLE: $t"; exit 1; }
done

python tools/check_inputs.py --trusted-manifest "$TRUSTED"
python src/run_lab.py --help
```

**Điều kiện PASS:** trusted input hợp lệ; `state.json` tồn tại; cả ba bảng Delta có thể đọc; runner thực sự nạp `templates/student_impl.py` đã triển khai hai hàm (không phải stub).

Không thay trusted manifest bằng manifest tiện ích nằm trong ZIP. Đoạn `test -d` ở trên áp dụng cho filesystem local; với S3A, xác minh bảng bằng công cụ và quyền truy cập phù hợp với môi trường nhóm.

### Kiểm tra state và phiên bản trước khi ghi

```bash
python - <<'PY'
import json, os
from pathlib import Path
from delta.tables import DeltaTable
import sys

sys.path.insert(0, "src")
from session import make_spark

cfg = json.loads(Path(os.environ["CFG"]).read_text())
state = Path(cfg["evidence_dir"]) / "state.json"
assert state.is_file(), f"MISSING_STATE: {state}"
data = json.loads(state.read_text())
assert not data.get("run_ids", {}).get("incremental"), "INCREMENTAL_ALREADY_RECORDED"
for name in ("bronze", "silver", "gold"):
    expected_path = f'{os.environ["TABLES"].rstrip("/")}/{name}'
    assert data.get(f"{name}_path", "").rstrip("/") == expected_path, f"STATE_PATH_MISMATCH: {name}"
    assert data.get(f"{name}_initial_version") == 0, f"UNEXPECTED_INITIAL_VERSION: {name}"

print("STATE:", state)
print(
    "VERSION_FIELDS:",
    {
        k: v
        for k, v in json.loads(state.read_text()).items()
        if "version" in k.lower()
    }
)

spark = make_spark(cfg)

try:
    for name, expected in (
        ("bronze", 9610),
        ("silver", 9610),
        ("gold", 16),
    ):
        path = f'{os.environ["TABLES"]}/{name}'
        latest = (
            DeltaTable.forPath(spark, path)
            .history(1)
            .select("version")
            .first()[0]
        )
        count = spark.read.format("delta").load(path).count()

        print(name, "version=", latest, "rows=", count)
        assert latest == 0 and count == expected, \
            f"NOT_FRESH_BASELINE: {name}"

    print("BASELINE_READY")

finally:
    spark.stop()
PY
```

Với dữ liệu course fallback, baseline Task 2 ban đầu gồm **Bronze 9.610**, **Silver 9.610**, **Gold 16 nhóm**, đều ở **version 0**.

Nếu bảng đã có version mới hơn hoặc state ghi nhận incremental, **dừng**; không chạy lại như một lần khởi tạo mới.

Muốn tái lập từ đầu, dùng một run/storage root mới và thực hiện bootstrap Task 2 trước.

---

## 4. Bước 1 — Chạy unit test cho hai hàm

Test `choose_revisions` đọc dữ liệu starter và `trusted_lab3_inputs.json` tại root dự án. Chuẩn bị bản manifest đã được TA xác thực độc lập tại vị trí đó và kiểm tra nó trùng byte với `$TRUSTED`; không đưa manifest hay dataset vào Git. Test `merge_silver` ghi Delta vào fixture tạm riêng, không ghi vào Bronze/Silver/Gold của lần chạy chính thức.

```bash
python - "$TRUSTED" <<'PY'
import hashlib, sys
from pathlib import Path

trusted = Path(sys.argv[1])
local = Path("trusted_lab3_inputs.json")
assert trusted.is_file() and local.is_file(), "MISSING_TRUSTED_MANIFEST"
assert hashlib.sha256(trusted.read_bytes()).digest() == hashlib.sha256(local.read_bytes()).digest(), "TRUSTED_MANIFEST_MISMATCH"
PY

python -m unittest discover -s tests -p 'test_choose_revisions.py' -v

# Thư mục test phải mới và nằm trực tiếp dưới lab3-merge-tests trong temp.
# Tạo tên mới mỗi lần chạy; không tự tạo thư mục fixture trước khi chạy test.
export LAB3_MERGE_TEST_ROOT="$(python -c 'import tempfile, uuid; from pathlib import Path; print(Path(tempfile.gettempdir()) / "lab3-merge-tests" / ("task3-" + uuid.uuid4().hex))')"
python -m unittest discover -s tests -p 'test_merge_silver.py' -v
```

**Kiểm tra:**

- `choose_revisions`: chọn đúng winner theo ba khóa ưu tiên, bảo toàn schema/provenance, kết quả không đổi khi đảo input hoặc repartition.
- `merge_silver`: INSERT, UPDATE, stale/equal NO-OP, tie-breaking, Decimal/UTC, bảo toàn provenance và replay tương đương logic.

Bộ kiểm thử tham chiếu có **10 test** cho `choose_revisions` và **13 test** cho `merge_silver`.

Cần xem kết quả chạy thực tế; không coi PASS chỉ vì số lượng test khớp.

---

## 5. Bước 2 — Chạy incremental

Chỉ chạy khi baseline vẫn chưa xử lý batch, config/state đúng và không có tiến trình khác ghi cùng ba bảng.

```bash
python src/run_lab.py \
  --config "$CFG" \
  --trusted-manifest "$TRUSTED" \
  --step incremental
```

**Kết quả dự kiến với batch course fallback:**

| Chỉ số | Giá trị |
|---|---:|
| Revision vật lý | 140 |
| Winner duy nhất | 130 |
| INSERT | 100 |
| UPDATE | 20 |
| Stale NO-OP | 10 |
| Bronze sau nhận batch | 9.750 receipts |
| Silver sau MERGE | 9.710 dòng |
| Gold sau cập nhật | 20 nhóm |

Số dòng Silver tăng **100** vì UPDATE chỉ thay giá trị dòng hiện có, còn NO-OP không thêm dòng.

Dữ liệu Gold phải được tính từ một **Silver version cụ thể**, lưu trong state.

> **Lưu ý:** Không chạy lại nguyên lệnh incremental một cách tùy tiện để thử replay. Nếu lỗi giữa chừng, giữ log, đọc Delta history/state để xác định bước đã commit. Chỉ thực hiện replay theo chức năng/test đã được kiểm tra và không làm mất evidence cũ.

---

## 6. Bước 3 — Kiểm chứng M01–M08

| ID | Nội dung | Bằng chứng cần kiểm tra |
|---|---|---|
| **M01** | Chọn winner | 140 dòng → 130 winner, không trùng `record_id`, 10 revision bị loại |
| **M02** | Quyết định MERGE | 100 INSERT, 20 UPDATE, 10 stale NO-OP; đối chiếu row trước/sau |
| **M03** | Determinism | Đảo thứ tự input/repartition không đổi dữ liệu và provenance của winner |
| **M04** | Bronze replay | Không thêm receipt trùng `(source_object, source_row, source_sha256)`; so nội dung logic, không chỉ count |
| **M05** | Silver replay | Dữ liệu/schema/provenance trước và sau replay tương đương; Delta version có thể tăng |
| **M06** | Time Travel | Đọc lại Silver version ban đầu và so với handoff |
| **M07** | Invalid batch rejection | Từ chối batch sai trước ghi; đối chiếu history và digest trước/sau |
| **M08** | Gold consistency | So từng nhóm `(site, event_date)` và từng trường Gold với tính toán độc lập từ Silver version nguồn |

**M07 gồm ba tình huống:** nhiệt độ ngoài khoảng hợp lệ, thiếu nhiệt độ, cột không được phép.

Phép kiểm tra validator cô lập **không đồng nghĩa** đã kiểm thử end-to-end đường đi của batch lỗi qua toàn bộ runner; phải ghi đúng phạm vi đã thực hiện.

### 6.1. Đọc số dòng và Delta history

```bash
python - <<'PY'
import json, os, sys
from pathlib import Path
from delta.tables import DeltaTable
from pyspark.sql import functions as F

sys.path.insert(0, "src")
from session import make_spark

cfg = json.loads(Path(os.environ["CFG"]).read_text())
spark = make_spark(cfg)

try:
    root = os.environ["TABLES"]

    for name in ("bronze", "silver", "gold"):
        path = f"{root}/{name}"
        df = spark.read.format("delta").load(path)
        history = DeltaTable.forPath(spark, path).history(5)

        print(name, "latest_rows=", df.count())
        history.select(
            "version", "operation"
        ).show(truncate=False)

    s0 = (
        spark.read.format("delta")
        .option("versionAsOf", 0)
        .load(f"{root}/silver")
    )

    latest = (
        spark.read.format("delta")
        .load(f"{root}/silver")
    )

    dup = (
        latest.groupBy("record_id")
        .count()
        .where(F.col("count") > 1)
        .count()
    )

    print("silver_v0_rows=", s0.count())
    print("duplicate_record_ids=", dup)

    assert dup == 0

finally:
    spark.stop()
PY
```

Lệnh trên là **kiểm tra nhanh**, không thay cho kiểm chứng logic và provenance đầy đủ của M01–M08.

### 6.2. Kiểm tra evidence và version binding

```bash
python - <<'PY'
import json, os
from pathlib import Path

cfg = json.loads(Path(os.environ["CFG"]).read_text())
ev = Path(cfg["evidence_dir"])

print("EVIDENCE_DIR:", ev)

for name in (
    "state.json",
    "incremental.json",
    "input_contract_tests.json",
    "lineage.jsonl",
):
    p = ev / name
    print(name, "FOUND" if p.is_file() else "NOT_FOUND")

state = ev / "state.json"

if state.is_file():
    data = json.loads(state.read_text())
    print(
        "VERSION_FIELDS:",
        {
            k: v
            for k, v in data.items()
            if "version" in k.lower()
        }
    )
PY
```

Cần xác định trong state **Gold version nào được tạo từ Silver version nào**.

Không tự sử dụng `latest Silver` làm nguồn Gold nếu đã có Silver replay ở version mới hơn.

Các file evidence thường bao gồm `incremental.json`, `state.json`, `input_contract_tests.json`, `lineage.jsonl`, log runner và các kết quả đối chiếu độc lập.

Tên/đường dẫn cuối cùng phải căn cứ vào runner thực tế.

---

## 7. Replay và Time Travel — Cách hiểu kết quả

- **Bronze replay:** cùng receipt vật lý không được ghi trùng lần hai.
- **Silver replay:** MERGE cùng batch không được thay đổi bản ghi hiện hành, provenance hoặc tổng số `record_id`. Delta history có thể tạo thêm version dù nội dung logic không đổi.
- **Time Travel:** dùng `versionAsOf` để đọc snapshot Silver trước MERGE, chứng minh lịch sử vẫn tồn tại.
- **Gold consistency:** Gold phải khớp với Silver **đúng version nguồn đã pin**, không chỉ khớp tổng số nhóm.

Ví dụ phiên bản của một lần chạy sạch:

```text
Silver@0 (baseline)
    |
    v
Silver@1 (sau MERGE) ----> Gold@1 (tạo từ Silver@1)
    |
    v
Silver@2 (replay, logic giống Silver@1)
```

Các số version này mang tính **minh họa cho chuỗi thao tác**; khi kiểm chứng phải lấy version thực tế từ state và Delta history.

---

## 8. Lỗi thường gặp

| Hiện tượng | Cách xử lý |
|---|---|
| `student_impl.py` vẫn là stub | Kiểm tra đúng file được runner import và revision code đã tích hợp |
| Không thấy `state.json` hoặc ba bảng Delta | Kiểm tra lại config/run label; Task 2 bootstrap phải hoàn tất |
| Baseline không còn ở version 0 | Dừng; không chạy incremental như lần đầu, tạo run mới nếu cần |
| `AMBIGUOUS_REVISION_ORDER` | Có revision trùng cả ba khóa ưu tiên; không chọn ngẫu nhiên |
| MERGE tạo nhiều dòng cùng `record_id` | Bảo đảm nguồn chỉ có một winner/key trước MERGE |
| Bản cũ ghi đè bản mới | Kiểm tra strict UPDATE predicate |
| Replay tăng Delta version | So canonical contents và provenance; không kết luận sai chỉ do version tăng |
| Không đọc được Silver@0 | Kiểm tra đúng table path/history; không xóa `_delta_log`, không VACUUM |
| Gold lệch Silver | Kiểm tra Gold source Silver version, UTC/Decimal và đối chiếu theo từng nhóm |
| Lệnh Bash có ký tự `\r` | Chuẩn hóa LF hoặc gõ lại lệnh; kiểm Delta history trước khi rerun |
| Runtime thiếu PySpark/Delta hoặc sai Java | Kích hoạt lại môi trường đã dùng cho Task 1–2 và kiểm phiên bản |

---

## 9. Checklist hoàn thành Task 3

- [ ] Trusted manifest và dữ liệu revision được kiểm tra thành công.
- [ ] Baseline Bronze/Silver/Gold và `state.json` hợp lệ trước incremental.
- [ ] Runner nạp đúng hai hàm `choose_revisions`, `merge_silver`.
- [ ] Unit test của hai hàm PASS.
- [ ] `choose_revisions` chọn đúng một winner mỗi `record_id`, không phụ thuộc thứ tự file.
- [ ] `merge_silver` thực hiện đúng INSERT/UPDATE/NO-OP và bảo toàn provenance.
- [ ] Incremental hoàn tất, kết quả có log và Delta history.
- [ ] M01–M08 có bằng chứng thực thi; ghi rõ phạm vi kiểm thử M07.
- [ ] Bronze/Silver replay tương đương logic, không tăng bản ghi trùng.
- [ ] Silver baseline đọc lại được bằng Time Travel.
- [ ] Gold khớp từng nhóm với Silver version nguồn trong state.
- [ ] Evidence được lưu, không ghi đè kết quả lỗi và không chỉnh sửa trusted input/Delta log.

**Tóm tắt:** `choose_revisions` quyết định *revision nào thắng trong batch*; `merge_silver` quyết định *winner đó có được thêm/cập nhật trong Silver hay không*. Incremental, replay, Time Travel và M01–M08 chứng minh pipeline cập nhật dữ liệu chính xác và có khả năng tái lập.
