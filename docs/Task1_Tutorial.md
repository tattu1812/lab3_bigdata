
## 0. Chuẩn bị

### 0.1. Phần mềm và phiên bản

| Thành phần | Yêu cầu của lab | Môi trường đã thử thành công |
|---|---|---|
| Hệ điều hành | Linux/WSL2 thuận tiện | Ubuntu 26.04.1 LTS (WSL2) |
| Python | 3.11 | 3.11.18 |
| Java | 17 hoặc 21 | OpenJDK 17.0.20.1 |
| PySpark | 4.0.1 | 4.0.1 |
| Delta-Spark | 4.0.1 | 4.0.1 |
| Hadoop runtime | theo Spark bundle | 3.4.1 (đã quan sát) |

Cần Internet cho bước cài dependencies và tải các Delta JAR (lần Spark chạy đầu nếu chưa cache). Có thể dùng Java 21 nếu Java 17 không khả dụng. Không cần thay Java/Python mặc định của hệ thống.

### 0.2. Ba file từ giảng viên

1. `Lab3-Student-Starter.zip` — mã nguồn + dữ liệu + schema.
2. `trusted_lab3_inputs.json` — danh sách 16 artifact kèm bytes/hash đáng tin cậy.
3. `trusted_lab3_inputs.sha256` — checksum của file JSON bên trên.

**Không** lấy `manifests/lab3_inputs_convenience.json` (nằm sẵn trong ZIP) thay cho trusted JSON được TA phát độc lập.

---

## 1. Chép file từ Windows sang WSL2 và giải nén

Mở terminal Ubuntu/WSL2:

```bash
mkdir -p "$HOME/projects"
cd "$HOME/projects"
```

Nếu chưa biết file ZIP nằm ở đâu trên Windows:

```bash
find /mnt/c/Users -maxdepth 5 -iname 'Lab3-Student-Starter.zip' 2>/dev/null
```

**Cách dễ nhất, không phụ thuộc tên tài khoản Windows:** Mở Explorer tại thư mục WSL bằng:

```bash
cd "$HOME/projects"
explorer.exe .
```

Sau đó kéo thả **cả 3 file** từ Downloads Windows vào Explorer vừa mở. Có thể dùng lệnh `cp` từ `/mnt/c/Users/<TEN_WINDOWS>/Downloads/` nếu biết chính xác đường dẫn; `<TEN_WINDOWS>` phải thay bằng tài khoản Windows của mỗi người.

Kiểm tra:

```bash
ls -lh "$HOME/projects/"Lab3-Student-Starter.zip \
  "$HOME/projects/"trusted_lab3_inputs.json \
  "$HOME/projects/"trusted_lab3_inputs.sha256
```

Giải nén (ZIP có **một thư mục con `Lab3-Student-Starter`**):

```bash
mkdir -p "$HOME/projects/lab3-bigdata"
unzip "$HOME/projects/Lab3-Student-Starter.zip" -d "$HOME/projects/lab3-bigdata"
cd "$HOME/projects/lab3-bigdata/Lab3-Student-Starter"
pwd
ls
```

Cần thấy `README.md`, `requirements.txt`, `config/`, `data/`, `manifests/`, `schemas/`, `src/`, `templates/`, `tools/`, ...

**Nếu báo `No such file or directory`:** Kiểm tra lại bước chép ZIP, và nhớ đường dẫn dự án có **hai tầng** `lab3-bigdata/Lab3-Student-Starter/`.

---

## 2. Kiểm tra trusted manifest và 16 artifact — H01 (phần integrity)

Kiểm tra checksum của trusted JSON từ đúng thư mục chứa 2 file:

```bash
cd "$HOME/projects"
sha256sum -c trusted_lab3_inputs.sha256
```

**PASS kỳ vọng:** `trusted_lab3_inputs.json: OK`. Nếu `FAILED`, tải lại bản TA phát, không sửa file để ép qua.

Chạy công cụ chính thức từ thư mục dự án:

```bash
cd "$HOME/projects/lab3-bigdata/Lab3-Student-Starter"
export TRUSTED="$HOME/projects/trusted_lab3_inputs.json"
python3 tools/check_inputs.py --trusted-manifest "$TRUSTED"
```

**PASS kỳ vọng:**

```text
PASS: all manifest bytes/hashes and transport contracts match.
Handoff rows: 9610; incoming physical revisions: 140.
This is an input check, NOT a Spark/Delta/Kubernetes execution test.
```

Đây **chưa** phải kiểm tra runtime Spark/Delta; nó xác minh 16 artifact và hợp đồng dữ liệu đầu vào. Không cần tạo/sinh lại dữ liệu.

---

## 3. Cài Python 3.11 + Java 17 và môi trường ảo

### 3.1. Xem máy đang có gì

```bash
cat /etc/os-release
python3 --version
java -version
command -v uv || true
```

Nếu đã có Python 3.11 và Java 17/21 hợp lệ, có thể bỏ qua phần cài tương ứng.

### 3.2. Java 17 (đã chạy thành công trên Ubuntu 26.04)

```bash
sudo apt update
sudo apt install -y openjdk-17-jdk
```

Dùng Java 17 **cho terminal Lab 3** (không đổi default toàn hệ thống):

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"
java -version
```

Nếu máy không có đường dẫn trên, kiểm tra bằng `ls /usr/lib/jvm/` và chọn đúng JDK được cài. Với Java 21, đặt `JAVA_HOME` trỏ tới JDK 21 phù hợp.

### 3.3. Cài `uv` và Python 3.11 (nếu chưa có)

Ubuntu 26.04 đã thử nghiệm không có `python3.11` trong repository mặc định; dùng `uv` để cài độc lập:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"
uv --version
uv python install 3.11
uv python list --only-installed
```

Trên hệ khác đã có Python 3.11, vẫn có thể dùng `uv` để tạo môi trường, không cần cài Python mới.

### 3.4. Tạo `.venv` và cài thư viện

```bash
cd "$HOME/projects/lab3-bigdata/Lab3-Student-Starter"
uv venv --python 3.11 .venv
source .venv/bin/activate
uv pip install -r requirements.txt
```

**Kiểm tra (phải đúng các phiên bản đề bài):**

```bash
python --version
java -version
python -c 'import pyspark; print("Spark:", pyspark.__version__)'
python -c 'import importlib.metadata as m; print("Delta:", m.version("delta-spark"))'
which python
```

Kỳ vọng: Python **3.11.x**, Java **17 hoặc 21**, Spark **4.0.1**, Delta **4.0.1**, `which python` nằm trong `.venv/bin/python`.

> **Sau khi mở terminal mới**, cần chạy lại `source .venv/bin/activate`, thiết lập `JAVA_HOME` và `PATH`. Không dùng `sudo pip` hoặc thay `python3` của hệ thống.

---

## 4. Tạo cấu hình riêng cho từng máy

**Không sửa `config/local.json` gốc.** Tạo file config riêng cho mỗi người. Ví dụ đặt tên `config/team.json` **tại máy riêng**:

```bash
cd "$HOME/projects/lab3-bigdata/Lab3-Student-Starter"
cp config/local.json config/team.json
python - <<'PY'
import json
from pathlib import Path
p = Path('config/team.json')
cfg = json.loads(p.read_text())
cfg['group_id'] = 'g01'               # Đổi thành mã nhóm thật nếu khác
cfg['run_label'] = 'member-task1-01'  # Đổi thành tên mỗi người, tránh đụng run
cfg['evidence_dir'] = 'evidence/member-task1-01'
cfg['spark_master'] = 'local[2]'
p.write_text(json.dumps(cfg, indent=2) + '\n')
print(p.read_text())
PY
```

**Mỗi người cần đổi** `run_label` và `evidence_dir` cho riêng mình, ví dụ `khanh-task1-01`, `trung-task1-01`. Giữ đúng `group_id` của nhóm. Mỗi lần chạy thử mới nên dùng `run_label` mới nếu đã tạo bảng/dữ liệu ở thư mục đích; không xóa Delta history để che lỗi.

Cấu hình local mẫu của TA có `lakehouse_root=/tmp/lab3-lakehouse`, `store_id=local-course-fixture`, `shuffle_partitions=8` và `extra_maven_packages=[]`. Giữ các trường này trừ khi có hướng dẫn riêng. **Local file storage không thay thế evidence về object storage S3.**

---

## 5. Chạy Task 1 — `preflight` chính thức

```bash
cd "$HOME/projects/lab3-bigdata/Lab3-Student-Starter"
source .venv/bin/activate
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"
export TRUSTED="$HOME/projects/trusted_lab3_inputs.json"
export CFG='config/team.json'

python src/run_lab.py \
  --config "$CFG" \
  --trusted-manifest "$TRUSTED" \
  --step preflight
```

Nếu dùng Java 21, chỉnh `JAVA_HOME` tương ứng. Lần đầu Spark có thể tải Delta JAR từ Maven Central.

**PASS kỳ vọng ở dòng cuối:**

```text
preflight: COMPLETE; evidence: evidence/member-task1-01
```

`preflight` tạo Delta smoke table có 8 dòng, thử append cột lạ (phải bị từ chối), xác minh 8 dòng vẫn giữ nguyên, kiểm tra input contract và ghi dấu runtime/worker. Preflight không ghi ba bảng Bronze/Silver/Gold vận hành.

---

## 6. Xem evidence và đánh giá H01–H04

Thay `member-task1-01` bằng `evidence_dir` đã đặt ở bước 4:

```bash
EV='evidence/member-task1-01'
ls -lh "$EV"
python -m json.tool "$EV/preflight.json"
python -m json.tool "$EV/delta_schema_probe.json"
python -m json.tool "$EV/runtime_preflight.json"
python -m json.tool "$EV/input_integrity.json"
python -m json.tool "$EV/input_contract_tests.json"
```

| File | Bằng chứng cần thấy |
|---|---|
| `input_integrity.json` | 16 đối tượng, tất cả `passed: true`; trusted manifest hash |
| `input_contract_tests.json` | 3 case `VALUE`, `REQUIRED_VALUE`, `SCHEMA_ALLOWLIST` PASS; không ghi (`writes_performed=0`) |
| `preflight.json` | `status: PASS`, `fixture_rows: 9610`, `new_batch_rows: 140` |
| `delta_schema_probe.json` | `rejected_as_analysis_error: true`, `rows_after: 8`, `passed: true` |
| `runtime_preflight.json` | Python/Spark/Delta/Java/Hadoop, `application_id`, UTC, master thật |
| `worker_probe.json` | Host và danh sách `partition_tasks` thực tế; không suy luận số worker từ số task |
| `run_preflight.json`, `lineage.jsonl`, `code_versions/` | Bằng chứng bổ sung của lần thực thi |

**Lệnh tổng hợp nhanh, không in dữ liệu nhạy cảm:**

```bash
python - <<'PY'
import json
from pathlib import Path
p = Path('evidence/member-task1-01')
def read(name): return json.loads((p/name).read_text())
checks = read('input_integrity.json')['checks']
pre = read('preflight.json')
probe = read('delta_schema_probe.json')
run = read('runtime_preflight.json')
worker = read('worker_probe.json')
print('Artifact pass:', sum(bool(x['passed']) for x in checks), '/', len(checks))
print('Preflight:', pre['status'], '| Handoff:', pre['fixture_rows'], '| Revisions:', pre['new_batch_rows'])
print('Delta schema:', probe['passed'], '| Rejected:', probe['rejected_as_analysis_error'], '| Rows:', probe['rows_after'])
print('Runtime:', {k:run.get(k) for k in ('python','spark','delta_spark','java','hadoop','spark_master','application_id','timezone','storage_mode')})
print('Worker hosts:', worker.get('distinct_hosts'), '| Partition tasks:', len(worker.get('partition_tasks',[])))
PY
```

**Ví dụ đã quan sát trên máy Tú (không được sao chép làm kết quả của mình):** 16/16 artifact PASS; `preflight PASS`; 9.610 handoff; 140 revisions; Delta rejection PASS, 8 dòng giữ nguyên; Python 3.11.18 / Spark 4.0.1 / Delta 4.0.1 / Java 17.0.20.1 / Hadoop 3.4.1; `local[2]`; 1 host và 24 partition tasks. `application_id` và đường dẫn smoke sẽ khác theo lần chạy.

---

## 7. H02: phân biệt handoff và truy vết một dòng gốc

Xem manifest:

```bash
python -m json.tool data/handoff/handoff_manifest.json
```

Đối với bộ starter này: `handoff_id=lab2-research-telemetry-v1-reference-handoff`, `handoff_kind=course_fallback_not_student_approval`, `row_count=9610`, 10 cột và `temperature_type=DECIMAL(8,2)`. **Không gọi CSV fallback là team-approved Parquet.** Logical SHA-256 trong manifest là `c50f8252bfb91890975d26b16fe1d2b32f0e283a58e46bd16260b4a3cca0bf0d`; kiểm tra logic bằng runner chính thức, không chỉ đọc chuỗi digest.

**Demo provenance riêng:** chọn 1 bản ghi từ curated (ví dụ đầu tiên) và đối chiếu với nguồn theo `source_object`, `source_row` (ordinal 1-based, không tính header) và `source_sha256`. Không in payload thô hoặc email.

```bash
python - <<'PY'
import csv, hashlib
from pathlib import Path
with open('data/handoff/curated.csv', newline='') as f:
    r = next(csv.DictReader(f))
source = Path('data/lab2_sources') / r['source_object']
assert source.is_file(), 'SOURCE_MISSING'
actual_hash = hashlib.sha256(source.read_bytes()).hexdigest()
print('record_id:', r['record_id'])
print('source_object:', r['source_object'])
print('source_row:', r['source_row'])
print('source_sha256_matches:', actual_hash == r['source_sha256'])
if source.suffix.lower() == '.csv':
    with source.open(newline='') as f:
        source_rows = list(csv.DictReader(f))
    source_row = source_rows[int(r['source_row']) - 1]
    print('source_record_id_matches:', source_row.get('record_id') == r['record_id'])
else:
    print('Non-CSV source: check ordinal with its source format parser')
PY
```

**Ví dụ đã quan sát:** `R000381` → `observations_a.csv` → `source_row=381` → ID trùng và source SHA-256 trùng; `is_late=true` vì 30 phút = 1800 giây > 900 giây. Với `.jsonl`, cách truy ordinal cần thích ứng theo định dạng, không đọc bằng `csv.DictReader`.

### Kiểm tra 10 cột handoff

```bash
python - <<'PY'
import csv,json
with open('data/handoff/curated.csv',newline='') as f:
    names=csv.DictReader(f).fieldnames
with open('data/handoff/handoff_manifest.json') as f:
    manifest=json.load(f)
print('column_count:',len(names))
print('names_and_order_match:',names==manifest['columns'])
print('handoff_kind:',manifest['handoff_kind'])
PY
```

Kỳ vọng: `10`, `True`, `course_fallback_not_student_approval`.

---

## 8. Kiểm tra bổ sung schema bằng Spark (H01)

Mục này kiểm tra **DataFrame được loader của starter tạo ra**, gồm 10 cột handoff và `event_date` (11 cột Silver). Đây là minh chứng bổ sung, **không thay cho kiểm tra bảng Delta Silver sau bootstrap của Task 2**.

Chạy khi đã kích hoạt `.venv`, Java 17 và đứng ở thư mục dự án:

```bash
python - <<'PY'
import sys, json
from pathlib import Path
from pyspark.sql import types as T
sys.path.insert(0, 'src')
from common import load_csv, load_registry, validate_rows
from run_lab import transport_df
from session import make_spark

cfg=json.loads(Path('config/team.json').read_text())
spark=make_spark(cfg)
try:
    rows=validate_rows(load_csv('data/handoff/curated.csv'),
                       load_registry('data/handoff/sensors.csv'), True)
    df=transport_df(spark,rows)
    contract=json.loads(Path('schemas/silver_schema.json').read_text())
    types={'STRING':T.StringType(),'TIMESTAMP':T.TimestampType(),
           'DECIMAL(8,2)':T.DecimalType(8,2),'BOOLEAN':T.BooleanType(),
           'BIGINT':T.LongType(),'DATE':T.DateType()}
    specs=contract['columns']; actual=df.schema.fields
    details=[{'column':exp['name'],
              'passed':got.name==exp['name'] and got.dataType==types[exp['type']],
              'actual':got.dataType.simpleString()}
             for got,exp in zip(actual,specs)]
    count=df.count(); tz=spark.conf.get('spark.sql.session.timeZone')
    ok=len(actual)==len(specs)==11 and all(x['passed'] for x in details) and count==9610 and tz=='UTC'
    result={'schema_pass':ok,'expected_columns':len(specs),
            'actual_columns':len(actual),'row_count':count,'timezone':tz,'columns':details}
    out=Path(cfg['evidence_dir'])/'spark_schema_check.json'
    out.parent.mkdir(parents=True,exist_ok=True)
    out.write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result,indent=2))
    assert ok, 'SCHEMA_OR_ROW_COUNT_FAILED'
    print('PASS: Spark schema, count and UTC verified')
finally:
    spark.stop()
PY
```

**PASS kỳ vọng:** `schema_pass=true`, `actual_columns=11`, `row_count=9610`, `timezone=UTC` và 11 `passed=true`. Đường dẫn report theo `cfg['evidence_dir']` của từng người.

---


## 9. Lỗi thường gặp và cách xử lý

| Triệu chứng | Nguyên nhân thường gặp | Cách xử lý |
|---|---|---|
| `unzip: cannot find ...zip` | ZIP còn ở Downloads Windows, chưa copy vào WSL | `find /mnt/c/Users ...` hoặc `explorer.exe .`; chép đủ 3 file |
| `cd .../lab3-bigdata: No such file` | Chưa tạo thư mục hoặc chưa giải nén | Chạy lại bước 1, nhớ thư mục con `Lab3-Student-Starter` |
| `sha256sum ... FAILED` | File manifest/checksum sai hoặc bị thay đổi | Lấy lại file từ TA; **không sửa hash** |
| `python3.11: command not found` | Ubuntu không đóng gói Python 3.11 | `uv python install 3.11`, tạo `.venv` mới |
| `java -version` báo 25 | Terminal đang trỏ Java hệ thống | `export JAVA_HOME=...17...` và thêm `$JAVA_HOME/bin` vào đầu `PATH` |
| `ModuleNotFoundError: pyspark` | Chưa activate `.venv`/chưa install | `source .venv/bin/activate`; `uv pip install -r requirements.txt` |
| Delta JAR không resolve | Không truy cập Maven, proxy/mạng | Kiểm tra kết nối tới Maven Central, retry khi mạng ổn định; không tự đổi Delta version |
| Cảnh báo hostname loopback / native Hadoop | Spark trên WSL/local | Nếu lệnh cuối PASS, có thể ghi nhận là warning; không tự gọi là multi-node |
| File evidence không thấy | `evidence_dir` khác hoặc chưa hoàn tất preflight | `cat config/team.json`, tìm `evidence_dir`, xem thông báo cuối của runner |
| Smoke path đã tồn tại hoặc run conflict | Tái sử dụng run label/đường dẫn cũ | Dùng `run_label` mới; **không xóa** bảng/log cũ để che lỗi |


---

## 10. Kết quả tham chiếu (không phải kết quả bảo đảm trên mọi máy)

- Ubuntu 26.04.1 WSL2; Python 3.11.18; Java 17.0.20.1; Spark/Delta 4.0.1; Hadoop 3.4.1.
- `input_integrity`: 16/16 PASS; `input_contract_tests`: 3/3 PASS.
- `preflight`: PASS; 9.610 handoff, 140 revisions.
- `delta_schema_probe`: rejected=true, rows_after=8, passed=true.
- `spark_schema_check`: 11/11 cột PASS, 9.610 dòng, UTC.
- `worker_probe`: 1 host, 24 partition tasks; **không** phải 24 workers.
- Provenance demo: `R000381` → `observations_a.csv`, source_row 381; source hash và record ID khớp.

