# Task 2 — Dựng 3 tầng bảng Bronze / Silver / Gold (B01–B04)

Người phụ trách: **B – Khánh** (Silver/Gold). Trọng số: 30 phút / 15 điểm.

## Mục lục

**Phần I — Explain**
- [E1. Task 2 yêu cầu gì](#e1-task-2-yêu-cầu-gì)
- [E2. Ba tầng bảng nghĩa là gì](#e2-ba-tầng-bảng-nghĩa-là-gì)
- [E3. Hợp đồng tính Gold và cách `build_gold` hiện thực](#e3-hợp-đồng-tính-gold-và-cách-build_gold-hiện-thực)
- [E4. B03 — bảo toàn tổng](#e4-b03--bảo-toàn-tổng)
- [E5. B04 — vì sao AVG(trung bình site/ngày) ≠ SUM(sum)/SUM(count)](#e5-b04--vì-sao-avgtrung-bình-sitengày--sumsumsumcount)

**Phần II — Tutorial**
- [0. Chuẩn bị](#0-chuẩn-bị)
- [1. Cấu hình riêng cho Task 2](#1-cấu-hình-riêng-cho-task-2)
- [2. Hiện thực `build_gold`](#2-hiện-thực-build_gold)
- [3. Kiểm tra `build_gold` trước khi ghi bảng (không ghi Delta)](#3-kiểm-tra-build_gold-trước-khi-ghi-bảng-không-ghi-delta)
- [4. Chạy Task 2 — `bootstrap` chính thức](#4-chạy-task-2--bootstrap-chính-thức)
- [5. Xem evidence và đánh giá B01–B04](#5-xem-evidence-và-đánh-giá-b01b04)
- [6. Demo cá nhân (2 phút)](#6-demo-cá-nhân-2-phút)
- [7. Bàn giao cho người sau](#7-bàn-giao-cho-người-sau)
- [8. Lỗi thường gặp và cách xử lý](#8-lỗi-thường-gặp-và-cách-xử-lý)
- [9. Kết quả tham chiếu](#9-kết-quả-tham-chiếu)

---

# Phần I — Explain

## E1. Task 2 yêu cầu gì

Từ handoff Lab 2 (9.610 dòng, 10 cột), dựng **3 bảng Delta thật** và chứng minh chúng đúng.

| ID | Điều kiện đạt |
|---|---|
| B01 | 3 bảng Delta thật; lưu URI và version cụ thể |
| B02 | Silver giống hệt logic với handoff; `event_date` suy ra đúng theo UTC |
| B03 | Gold.n_observations cộng lại = số dòng Silver; các tổng nhiệt độ cộng lại = tổng Silver |
| B04 | Giải thích vì sao AVG(trung bình site/ngày) ≠ SUM(sum)/SUM(count) |

Việc của Khánh: viết `build_gold` (Spark DataFrame/SQL thuần, đúng 11 cột, đúng kiểu), chạy `--step bootstrap`, kiểm tra B03, viết B04, là **steward** trong lệnh release, và demo cá nhân.

**Cấm:** không Pandas, không Python UDF cho phép tính SQL cơ bản, không join QA reference để thay giá trị đo, không `collect()` trong `build_gold`.

## E2. Ba tầng bảng nghĩa là gì

| Tầng | Một dòng nghĩa là gì / khoá | Quy tắc quan trọng |
|---|---|---|
| Bronze | Một "biên nhận" vật lý; khoá = `source_object + source_row + source_sha256` | Giữ mọi phiên bản vật lý; replay không chèn lại biên nhận cũ |
| Silver | Một bản đo hiện hành cho mỗi `record_id` | Đủ 10 cột + `event_date` (UTC); không bỏ dòng trễ |
| Gold | Một nhóm theo `(site, event_date)` | 11 cột; lưu thành phần cộng được (sum, count) để rollup |

Bootstrap ghi Bronze từ handoff (nhãn `lab2-handoff`), tạo Silver từ Bronze, rồi tạo Gold bằng `build_gold` đọc **đúng version Silver vừa ghi**. Runner gọi `build_gold` một lần **trước** lần ghi đầu tiên: nếu hàm còn `NotImplementedError` thì dừng sớm, chưa có bảng nào bị tạo.

## E3. Hợp đồng tính Gold và cách `build_gold` hiện thực

| Cột Gold | Cách tính | Kiểu |
|---|---|---|
| `site`, `event_date` | khoá nhóm | STRING, DATE |
| `n_observations` | số `record_id` được giữ (`COUNT(*)`) | BIGINT |
| `sum_temperature_c` | `SUM(temperature_c)` | DECIMAL(18,2) |
| `mean_temperature_c` | `sum_temperature_c / n_observations` | DECIMAL(18,6) |
| `min_temperature_c`, `max_temperature_c` | `MIN`, `MAX` | DECIMAL(18,2) |
| `n_late` | số dòng `is_late = true` (cờ 0/1) | BIGINT |
| `late_rate` | `n_late / n_observations` | DECIMAL(18,6) |
| `n_above_25` | số dòng `temperature_c > 25` (lớn hơn **chặt**) | BIGINT |
| `n_sensors` | `COUNT(DISTINCT sensor_id)` | BIGINT |

Ba điểm dễ sai:
1. `mean` và `late_rate` tính từ **thành phần cộng được** (sum, count), không dùng `AVG()` rồi lưu lại, để bảo đảm `mean × count = sum` và rollup đúng.
2. `n_above_25` dùng `>` chứ không phải `>=`; nhiệt độ đúng 25,00 không được đếm.
3. `n_sensors` phải là số sensor **phân biệt** trong từng nhóm, không phải `COUNT(sensor_id)`.

## E4. B03 — bảo toàn tổng

Mỗi dòng Silver thuộc đúng **một** nhóm `(site, event_date)`, nên cộng các nhóm Gold phải lại ra Silver:

- Σ `n_observations` (Gold) = số dòng Silver (9.610 với CSV fallback).
- Σ `sum_temperature_c` (Gold) = tổng `temperature_c` của Silver.

Nếu lệch, nguyên nhân thường là: `site`/`event_date` NULL bị loại khỏi nhóm, `event_date` suy sai múi giờ, hoặc join làm nhân đôi dòng.

## E5. B04 — vì sao AVG(trung bình site/ngày) ≠ SUM(sum)/SUM(count)

Mỗi dòng Gold là trung bình của một nhóm có số quan sát `n_i` khác nhau.

- **Trung bình của các trung bình:** `AVG(mean_i) = (1/k) · Σ mean_i` — mỗi *nhóm* có trọng số bằng nhau, bất kể nhóm có 3 hay 300 quan sát.
- **Trung bình có trọng số:** `Σ sum_i / Σ n_i = Σ (n_i · mean_i) / Σ n_i` — mỗi *quan sát* có trọng số bằng nhau; đây mới bằng trung bình trên toàn bộ Silver.

Hai giá trị chỉ bằng nhau khi mọi nhóm có cùng `n_i`. Vì vậy Gold lưu thành phần cộng được (`sum_temperature_c`, `n_observations`): rollup lên ngày/tuần phải cộng sum và count rồi mới chia; **không** rollup được từ `mean_temperature_c` hay `late_rate` một mình.

Ví dụ: nhóm A có 1 quan sát = 10 °C; nhóm B có 9 quan sát cùng = 20 °C. `AVG(mean) = (10 + 20)/2 = 15,0` nhưng `SUM(sum)/SUM(count) = (10 + 180)/10 = 19,0`.

> Khi nộp, thay ví dụ giả định này bằng số thật từ lần chạy của nhóm (xem bước 5.3).

---

# Phần II — Tutorial

Hướng dẫn này nối tiếp `docs/Task1_Tutorial.md` (cùng môi trường WSL2, `.venv`, cấu hình `config/team.json`).

> **Quy ước các khối code (đọc trước khi copy):**
> - Khối mở đầu bằng `# CHẠY TRONG TERMINAL` là lệnh để dán vào terminal (bash). Chạy từ thư mục gốc dự án.
> - Khối mở đầu bằng `# KHÔNG CHẠY TRONG TERMINAL` là **mã nguồn hoặc nội dung file**: phải dán vào đúng file được nêu, dán vào terminal sẽ báo lỗi (vì terminal là bash, không phải Python).
> - Khối ghi `# CẦN SỬA` có giá trị giữ chỗ phải thay bằng giá trị thật trước khi chạy.
> - Đoạn "Kết quả mẫu" là **đầu ra để đối chiếu**, không gõ vào terminal.

## 0. Chuẩn bị

### 0.1. Điều kiện bắt buộc

| Điều kiện | Cách kiểm tra |
|---|---|
| Đã dựng môi trường như Task 1 (Python 3.11, Java 17/21, PySpark/Delta 4.0.1 trong `.venv`) | `docs/Task1_Tutorial.md`, mục 3; kiểm tra ở 0.2 |
| Có `data/` (giải nén ZIP theo `docs/Task1_Tutorial.md`, mục 1) | `cd "$HOME/projects/lab3-bigdata/Lab3-Student-Starter" && ls data/handoff` |
| Có `trusted_lab3_inputs.json` và `.sha256` từ giảng viên, nằm ở `$HOME/projects/` (**không** nằm trong thư mục dự án) | `cd "$HOME/projects" && sha256sum -c trusted_lab3_inputs.sha256` → `trusted_lab3_inputs.json: OK` |
| `check_inputs.py` PASS | Xem 0.3 |

Vị trí file theo Task 1:

```text
$HOME/projects/
├── Lab3-Student-Starter.zip
├── trusted_lab3_inputs.json
├── trusted_lab3_inputs.sha256
└── lab3-bigdata/Lab3-Student-Starter/   <- thư mục dự án (chạy mọi lệnh ở đây)
```

Về mặt kỹ thuật bootstrap không đọc `preflight.json` của Task 1 và tự kiểm tra input với trusted manifest. Tuy vậy vẫn nên để preflight (Tất Tú) PASS trước, để lỗi môi trường không lộ ra giữa lúc bootstrap, khi có thể đã có bảng ghi dở.

### 0.2. Mỗi lần mở terminal mới

```bash
# CHẠY TRONG TERMINAL (CẦN SỬA nếu thư mục dự án/JAVA_HOME của bạn khác Task 1)
cd "$HOME/projects/lab3-bigdata/Lab3-Student-Starter"
source .venv/bin/activate
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"
export TRUSTED="$HOME/projects/trusted_lab3_inputs.json"
export CFG='config/team.json'
```

Mọi lệnh phải chạy từ **thư mục gốc dự án**, vì `evidence_dir` là đường dẫn tương đối. Nếu máy dùng Java 21 (Task 1 cho phép), chỉnh `JAVA_HOME` cho đúng JDK (`ls /usr/lib/jvm/`).

Kiểm tra môi trường (giống `docs/Task1_Tutorial.md`, mục 3.4):

```bash
# CHẠY TRONG TERMINAL
python --version
java -version
python -c 'import pyspark; print("Spark:", pyspark.__version__)'
python -c 'import importlib.metadata as m; print("Delta:", m.version("delta-spark"))'
which python
```

Kết quả đúng (đối chiếu, KHÔNG gõ vào terminal): Python `3.11.x`, Java `17` hoặc `21`, `Spark: 4.0.1`, `Delta: 4.0.1`, và `which python` nằm trong `.venv/bin/python`. Lệch thì quay lại `docs/Task1_Tutorial.md`, mục 3.

### 0.3. Kiểm tra input trước khi ghi bất cứ gì

```bash
# CHẠY TRONG TERMINAL
python tools/check_inputs.py --trusted-manifest "$TRUSTED"
```

Kết quả mẫu (đối chiếu, KHÔNG gõ vào terminal):

```text
PASS: all manifest bytes/hashes and transport contracts match.
Handoff rows: 9610; incoming physical revisions: 140.
```

---

## 1. Cấu hình riêng cho Task 2

Giống Task 1: **không sửa `config/local.json` gốc**, dùng `config/team.json` tại máy riêng, và mỗi lần chạy dùng `run_label` + `evidence_dir` mới.

Có hai loại lần chạy, **không dùng chung** `run_label`/`evidence_dir`:

| Loại | `run_label` | `evidence_dir` | Dùng để |
|---|---|---|---|
| Chạy thử cá nhân | `khanh-task2-01`, `-02`, ... | `evidence/khanh-task2-01`, ... | Gỡ lỗi `build_gold`; **không nộp** |
| Chạy chính thức của nhóm | nhãn nhóm đã thống nhất (vd `attempt01`) | `evidence` | C, D, E đọc `state.json` từ đây |

**Liên hệ với Task 1:** `config/team.json` đã được tạo ở Task 1 (`docs/Task1_Tutorial.md`, mục 4) với `run_label`/`evidence_dir` dạng `member-task1-01` (hoặc `khanh-task1-01`). Đoạn dưới **đổi hai giá trị đó** sang giá trị của Task 2. Thư mục evidence của Task 1 (`evidence/<tên task1>`) vẫn nguyên, **không xoá, không dùng lại**. Xem trước giá trị hiện tại:

```bash
# CHẠY TRONG TERMINAL
python -c "import json; c=json.load(open('config/team.json')); print({k: c[k] for k in ('group_id','run_label','evidence_dir','spark_master')})"
ls evidence
```

Tạo cấu hình cho lần chạy thử:

```bash
# CHẠY TRONG TERMINAL
cp -n config/local.json config/team.json   # -n: không ghi đè team.json đã có từ Task 1
python - <<'PY'
import json
from pathlib import Path
p = Path('config/team.json')
cfg = json.loads(p.read_text())
cfg['group_id'] = 'g01'                    # Đổi thành mã nhóm thật nếu khác
cfg['run_label'] = 'khanh-task2-01'        # Mỗi lần chạy mới: đổi số cuối
cfg['evidence_dir'] = 'evidence/khanh-task2-01'
cfg['spark_master'] = 'local[2]'
p.write_text(json.dumps(cfg, indent=2) + '\n')
print(p.read_text())
PY
```

Bảng Delta nằm ở `<lakehouse_root>/<group_id>/<run_label>/tables/{bronze,silver,gold}`. Đổi `run_label` là có bộ bảng mới, không đụng bảng cũ.

**Quy tắc để không che lỗi:**
- Bootstrap từ chối chạy nếu `<evidence_dir>/state.json` đã tồn tại (`USE_NEW_RUN_LABEL_OR_RESUME_NEXT_STEP`) → đổi `run_label` **và** `evidence_dir`, **không xoá** thư mục cũ.
- Không sửa `_delta_log`, không `VACUUM`, không đổi hash trong trusted manifest.
- Khi chạy fail, **giữ** `run_bootstrap_error.json` và `lineage.jsonl` (sự kiện `FAIL`/`ABORT`); đây là bằng chứng.
- Chỉ nộp evidence của lần chạy chính thức, không nộp evidence chạy thử.

---

## 2. Hiện thực `build_gold`

Hàm nằm trong `templates/student_impl.py`. **Mỗi máy/bản giải nén có file riêng**: nếu file ở máy này vẫn còn `raise NotImplementedError('TODO 1: build_gold')` thì bước 3 báo `NotImplementedError: TODO 1`. Lệnh dưới đây tự ghi `build_gold` vào đúng file đó, **chỉ thay đúng dòng của TODO 1** (TODO 2–5 giữ nguyên) và an toàn khi chạy lại (đã có thì in `SKIP`). Không cần file nào khác ngoài tutorial này.

```bash
# CHẠY TRONG TERMINAL (từ thư mục gốc dự án; dán nguyên khối, gồm cả dòng PY ở cuối)
python - <<'PY'
from pathlib import Path
p = Path('templates/student_impl.py')
s = p.read_text(encoding='utf-8')
old = "    raise NotImplementedError('TODO 1: build_gold')"
if old not in s:
    print('SKIP: build_gold đã được hiện thực (không còn NotImplementedError)')
    raise SystemExit
new = '''    from pyspark.sql import functions as F

    dec2 = 'decimal(18,2)'
    dec6 = 'decimal(18,6)'
    late_flag = F.when(F.col('is_late'), F.lit(1)).otherwise(F.lit(0))
    agg = (silver_df
           .groupBy('site', 'event_date')
           .agg(F.count(F.lit(1)).cast('bigint').alias('n_observations'),
                F.sum('temperature_c').cast(dec2).alias('sum_temperature_c'),
                F.min('temperature_c').cast(dec2).alias('min_temperature_c'),
                F.max('temperature_c').cast(dec2).alias('max_temperature_c'),
                F.sum(late_flag).cast('bigint').alias('n_late'),
                F.sum(F.when(F.col('temperature_c') > F.lit(25), F.lit(1)).otherwise(F.lit(0)))
                 .cast('bigint').alias('n_above_25'),
                F.countDistinct('sensor_id').cast('bigint').alias('n_sensors')))
    # mean/rate lấy từ thành phần cộng được (SUM / COUNT), không dùng AVG.
    return agg.select(
        'site', 'event_date', 'n_observations', 'sum_temperature_c',
        (F.col('sum_temperature_c') / F.col('n_observations')).cast(dec6).alias('mean_temperature_c'),
        'min_temperature_c', 'max_temperature_c', 'n_late',
        (F.col('n_late') / F.col('n_observations')).cast(dec6).alias('late_rate'),
        'n_above_25', 'n_sensors')'''
p.write_text(s.replace(old, new, 1), encoding='utf-8')
print('PATCHED: build_gold đã được ghi vào templates/student_impl.py')
PY
```

Kết quả mẫu (đối chiếu, KHÔNG gõ vào terminal): `PATCHED: ...` lần đầu, `SKIP: ...` nếu chạy lại.

Kiểm tra cú pháp và xác nhận đã hết `NotImplementedError` của TODO 1:

```bash
# CHẠY TRONG TERMINAL
python -c "import ast; ast.parse(open('templates/student_impl.py', encoding='utf-8').read()); print('syntax ok')"
grep -n "NotImplementedError('TODO 1" templates/student_impl.py || echo "OK: TODO 1 đã được hiện thực"
```

Kết quả mẫu: `syntax ok` và `OK: TODO 1 đã được hiện thực`. (Chữ `TODO 1` vẫn còn trong docstring; đó là bình thường.)

> Ý chính của `build_gold` (E3): `mean_temperature_c = sum / count` và `late_rate = n_late / count`, **không** gọi `AVG()`.

---

## 3. Kiểm tra `build_gold` trước khi ghi bảng (không ghi Delta)

Bước này tạo file `tools/task2_precheck.py` rồi chạy. Nó dựng DataFrame Silver từ loader của starter (giống Task 1, mục 8), gọi `build_gold`, đối chiếu schema với `schemas/gold_schema.json` và kiểm tra B03. **Không tạo bảng Delta.**

**3.1. Tạo file** (một lần; dán nguyên khối, gồm cả dòng `EOF` ở cuối):

```bash
# CHẠY TRONG TERMINAL (từ thư mục gốc dự án)
mkdir -p tools
cat > tools/task2_precheck.py <<'EOF'
"""Task 2: kiem tra build_gold TRUOC khi bootstrap. KHONG ghi bang Delta.
Chay tu thu muc goc du an: python tools/task2_precheck.py [--config config/team.json]
"""
import argparse, json, sys
from pathlib import Path
ROOT = Path.cwd()
sys.path.insert(0, str(ROOT / 'src')); sys.path.insert(0, str(ROOT / 'templates'))
from pyspark.sql import functions as F
from common import load_csv, load_registry, validate_rows
from run_lab import transport_df
from session import make_spark
import student_impl

ap = argparse.ArgumentParser(); ap.add_argument('--config', default='config/team.json'); a = ap.parse_args()
cfg = json.loads(Path(a.config).read_text())
spark = make_spark(cfg)
ok = False
try:
    rows = validate_rows(load_csv('data/handoff/curated.csv'), load_registry('data/handoff/sensors.csv'), True)
    silver = transport_df(spark, rows)
    gold = student_impl.build_gold(silver)

    contract = json.loads(Path('schemas/gold_schema.json').read_text())['columns']
    want = [(c['name'], c['type'].lower()) for c in contract]
    got = [(f.name, f.dataType.simpleString()) for f in gold.schema.fields]
    print('schema_match:', got == want)
    if got != want:
        print('want:', want); print('got :', got)

    n_gold = gold.agg(F.sum('n_observations')).first()[0]
    s_gold = gold.agg(F.sum('sum_temperature_c')).first()[0]
    n_silver = silver.count()
    s_silver = silver.agg(F.sum('temperature_c')).first()[0]
    print('B03 count:', n_gold, '==', n_silver, '->', n_gold == n_silver)
    print('B03 sum  :', s_gold, '==', s_silver, '->', s_gold == s_silver)
    ok = got == want and n_gold == n_silver and s_gold == s_silver
    print('PASS: build_gold schema and conservation' if ok else 'FAIL: xem cac dong tren')
finally:
    spark.stop()
raise SystemExit(0 if ok else 1)
EOF
```

**Kiểm tra file không bị cắt khi dán** (nên làm; nếu lệch thì dán lại khối tạo file ở trên):

```bash
# CHẠY TRONG TERMINAL
wc -l tools/task2_precheck.py
sha256sum tools/task2_precheck.py
python -m py_compile tools/task2_precheck.py && echo "compile ok"
```

Kết quả đúng (đối chiếu, KHÔNG gõ vào terminal): `40` dòng; sha256 là `52aaaebac9b224999690a069f7a1555bda2e92620e1a7593202b7b16bf88347c`; và `compile ok`.

**3.2. Chạy** (cần `.venv` + Java + `$CFG` như mục 0.2):

```bash
# CHẠY TRONG TERMINAL
python tools/task2_precheck.py --config "$CFG"
```

Kết quả mẫu (đối chiếu, KHÔNG gõ vào terminal):

```text
schema_match: True
B03 count: 9610 == 9610 -> True
B03 sum  : <tổng> == <tổng> -> True
PASS: build_gold schema and conservation
```

Nếu `FAIL` hoặc có lỗi, **không** sang bước 4. Nếu thấy `NotImplementedError: TODO 1`, quay lại bước 2.

---

## 4. Chạy Task 2 — `bootstrap` chính thức

### 4.1. Lần chạy thử cá nhân

Bootstrap đọc handoff là file CSV fallback của khoá học (`data/handoff/curated.csv`, 9.610 dòng). Đây **không phải** bản Parquet do nhóm duyệt; khi nộp, ghi đúng nguồn này.

```bash
# CHẠY TRONG TERMINAL
python src/run_lab.py \
  --config "$CFG" \
  --trusted-manifest "$TRUSTED" \
  --step bootstrap
```

**Kết quả mẫu ở dòng cuối (đối chiếu, KHÔNG gõ vào terminal):**

```text
bootstrap: COMPLETE; evidence: evidence/khanh-task2-01
```

### 4.2. Lần chạy chính thức của nhóm

Chạy khi nhóm đã thống nhất `run_label`, sau khi Task 1 PASS. Đổi `run_label` và `evidence_dir` trong `config/team.json` về giá trị nhóm thống nhất (ví dụ `attempt01`, `evidence`), rồi chạy lại đúng lệnh ở 4.1. **Chỉ một người chạy, một driver ghi một bảng tại một thời điểm.**

---

## 5. Xem evidence và đánh giá B01–B04

Thay `EV` bằng `evidence_dir` của lần chạy cần kiểm tra:

```bash
# CHẠY TRONG TERMINAL (CẦN SỬA: EV phải trỏ đúng evidence_dir của lần chạy cần xem)
EV='evidence/khanh-task2-01'
ls -lh "$EV"
python -m json.tool "$EV/bootstrap.json"
python -m json.tool "$EV/state.json"
```

**Kết quả mẫu bằng chứng sau bootstrap** — đây chỉ là đầu ra để đối chiếu (KHÔNG gõ vào terminal). Thấy được các file và giá trị này nghĩa là **bootstrap chạy xong, chưa phải Task 2 đã xong**: còn phải chạy 5.1 (B02, B03, B04) để có `OVERALL PASS`.

```text
$ ls -lh "$EV"
bootstrap.json            <- số dòng và version của 3 bảng
state.json                <- URI + version thật, hash handoff, hash code
gold_baseline.json        <- Gold baseline, mỗi nhóm một dòng, đúng 11 cột
silver_baseline.jsonl     <- Silver baseline (10 cột Lab 2, sắp theo record_id)
run_bootstrap.json        <- run ID
lineage.jsonl             <- sự kiện START / COMPLETE
runtime_bootstrap.json    <- runtime thật của lần chạy
input_integrity.json      <- kiểm tra input với trusted manifest
code_versions/            <- bản code đã chạy

$ python -m json.tool "$EV/bootstrap.json"
{
    "rows": 9610,
    "gold_rows": 16,
    "silver_version": 0,
    "gold_version": 0,
    "bronze_rows": 9610
}

$ python -m json.tool "$EV/state.json"      (các khoá cần có)
    bronze_path / silver_path / gold_path           = <lakehouse_root>/g01/<run_label>/tables/{bronze,silver,gold}
    bronze_initial_version / silver_initial_version / gold_initial_version = version thật (bootstrap mới: 0)
    gold_source_silver_version                      = silver_initial_version
    handoff_kind                                    = course_fallback_not_student_approval
    handoff_logical_sha256                          = c50f8252bfb91890975d26b16fe1d2b32f0e283a58e46bd16260b4a3cca0bf0d
    initial_implementation_sha256, run_ids.bootstrap = khác nhau theo lần chạy
```

Giá trị `gold_rows`, `rows`, `handoff_logical_sha256` là cố định theo dữ liệu khoá học; `run_ids`, `initial_implementation_sha256` và đường dẫn bảng khác nhau theo máy và `run_label`.

### 5.1. Kiểm tra tự động B01–B04

**5.1.1. Tạo file** `tools/task2_verify.py` (một lần; dán nguyên khối, gồm cả dòng `EOF` ở cuối):

```bash
# CHẠY TRONG TERMINAL (từ thư mục gốc dự án)
mkdir -p tools
cat > tools/task2_verify.py <<'EOF'
"""Task 2 (B01-B04) checker. Stdlib only. Run AFTER --step bootstrap.
Usage (from repo root): python tools/task2_verify.py --evidence <evidence_dir>
"""
import argparse, json
from collections import defaultdict
from decimal import Decimal
from pathlib import Path

p = argparse.ArgumentParser(); p.add_argument('--evidence', default='evidence'); a = p.parse_args()
ev = Path(a.evidence)
state = json.loads((ev / 'state.json').read_text(encoding='utf-8'))
gold = json.loads((ev / 'gold_baseline.json').read_text(encoding='utf-8'))
silver = [json.loads(l) for l in (ev / 'silver_baseline.jsonl').read_text(encoding='utf-8').splitlines() if l.strip()]
ok = True
def check(name, cond, detail=''):
    global ok; ok &= bool(cond); print(('PASS ' if cond else 'FAIL ') + name, detail)

# B01
for k in ('bronze_path', 'silver_path', 'gold_path', 'bronze_initial_version', 'silver_initial_version', 'gold_initial_version'):
    print(f'  {k} = {state[k]}')
check('B01 three Delta tables recorded with versions', all(k in state for k in ('bronze_path','silver_path','gold_path')))

# B03
cols = ['site','event_date','n_observations','sum_temperature_c','mean_temperature_c','min_temperature_c',
        'max_temperature_c','n_late','late_rate','n_above_25','n_sensors']
check('Gold has exactly 11 columns in order', all(list(r.keys()) == cols for r in gold))
n_g = sum(int(r['n_observations']) for r in gold); s_g = sum(Decimal(r['sum_temperature_c']) for r in gold)
n_s = len(silver); s_s = sum(Decimal(str(r['temperature_c'])) for r in silver)
check('B03 sum(n_observations) == Silver rows', n_g == n_s, f'{n_g} vs {n_s}')
check('B03 sum(sum_temperature_c) == Silver sum', s_g == s_s, f'{s_g} vs {s_s}')

# independent recompute of every group (pure Python, no Spark)
grp = defaultdict(list)
# silver_baseline.jsonl has the 10 Lab-2 columns only; event_date is derived from event_time_utc (UTC, 'YYYY-MM-DDT...Z')
for r in silver: grp[(r['site'], str(r['event_time_utc'])[:10])].append(r)
check('Gold group count == Silver group count', len(grp) == len(gold), f'{len(gold)} vs {len(grp)}')
bad = 0
for g in gold:
    rs = grp[(g['site'], g['event_date'])]; t = [Decimal(str(r['temperature_c'])) for r in rs]
    if not rs:
        bad += 1; continue
    exp = (len(rs), sum(t), min(t), max(t), sum(1 for r in rs if r['is_late'] in (True, 'true', 'True', 1)),
           sum(1 for x in t if x > 25), len({r['sensor_id'] for r in rs}))
    got = (int(g['n_observations']), Decimal(g['sum_temperature_c']), Decimal(g['min_temperature_c']), Decimal(g['max_temperature_c']),
           int(g['n_late']), int(g['n_above_25']), int(g['n_sensors']))
    if exp != got or abs(Decimal(g['mean_temperature_c']) - exp[1] / exp[0]) > Decimal('0.000001'): bad += 1
check('Every Gold group matches independent recompute', bad == 0, f'{bad} mismatched')

# B02: Gold event_date set must equal the UTC dates derived from Silver event_time_utc
check('B02 Gold (site,event_date) keys == keys derived from UTC event_time', set(grp) == {(g['site'], g['event_date']) for g in gold})

# B04 / demo: weighted vs unweighted by day
by_day = defaultdict(list)
for g in gold: by_day[g['event_date']].append(g)
print('\nB04 demo: day | weighted = SUM(sum)/SUM(n) | unweighted = AVG(site means) | diff')
for d in sorted(by_day):
    gs = by_day[d]; w = sum(Decimal(g['sum_temperature_c']) for g in gs) / sum(int(g['n_observations']) for g in gs)
    u = sum(Decimal(g['mean_temperature_c']) for g in gs) / len(gs)
    print(f'{d} | {w:.6f} | {u:.6f} | {w-u:+.6f}')
print('\nOVERALL', 'PASS' if ok else 'FAIL'); raise SystemExit(0 if ok else 1)
EOF
```

**Kiểm tra file không bị cắt khi dán** (nên làm; nếu lệch thì dán lại khối tạo file ở trên):

```bash
# CHẠY TRONG TERMINAL
wc -l tools/task2_verify.py
sha256sum tools/task2_verify.py
python -m py_compile tools/task2_verify.py && echo "compile ok"
```

Kết quả đúng (đối chiếu, KHÔNG gõ vào terminal): `60` dòng; sha256 là `76a6831a6bb6096625a001668984e343b89d5a6663f1681d447647aa291e6d9d`; và `compile ok`.

**5.1.2. Chạy** (`EV` như đầu mục 5):

```bash
# CHẠY TRONG TERMINAL
python tools/task2_verify.py --evidence "$EV"
```

Script chỉ dùng thư viện chuẩn (không cần Spark), kiểm tra:
- **B01:** in URI và version của 3 bảng.
- Gold đúng 11 cột, đúng thứ tự.
- **B03:** Σ `n_observations` = số dòng Silver; Σ `sum_temperature_c` = tổng Silver.
- **B02:** tập khoá `(site, event_date)` của Gold = tập suy ra từ ngày UTC của `event_time_utc` trong Silver.
- Tự tính lại **từng nhóm** (count, sum, min, max, n_late, n_above_25, n_sensors, mean) bằng Python thuần và so với Gold.

Kỳ vọng dòng cuối: `OVERALL PASS`.

### 5.2. Ghi version thật

Ghi lại (không giả định version ban đầu = 0):

```bash
# CHẠY TRONG TERMINAL
python - <<'PY'
import json
s = json.load(open('evidence/khanh-task2-01/state.json'))   # đổi đường dẫn cho đúng lần chạy
for k in ('bronze_path','silver_path','gold_path','bronze_initial_version',
          'silver_initial_version','gold_initial_version','gold_source_silver_version'):
    print(f'{k}: {s[k]}')
PY
```

### 5.3. B04 — số thật cho báo cáo

Phần cuối của `task2_verify.py` in từng ngày, định dạng như sau (mô tả đầu ra, KHÔNG gõ vào terminal):

```text
day | weighted = SUM(sum)/SUM(n) | unweighted = AVG(site means) | diff
```

Chọn **một ngày có chênh lệch rõ**, ghi: ngày nào, các `(sum_i, n_i)` của từng site, hai giá trị và hiệu số. Đây là số thay cho ví dụ giả định ở E5.

---

## 6. Demo cá nhân (2 phút)

Yêu cầu: tính lại trung bình có trọng số theo ngày từ (sum, count) của Gold và so với trung bình không trọng số của các trung bình site.

1. Chạy `python tools/task2_verify.py --evidence "$EV"`, chỉ vào bảng weighted vs unweighted.
2. Chọn một ngày chênh lệch, đọc `(sum_i, n_i)` từng site từ `gold_baseline.json`, tính tay `Σsum / Σn` và `AVG(mean_i)`.
3. Giải thích: AVG(mean_i) cho mỗi nhóm trọng số bằng nhau, còn `Σsum/Σn` cho mỗi quan sát trọng số bằng nhau → Gold lưu (sum, count).
4. Kiểm tra chéo (phần của Khánh): xem kết quả stale/tied của Trung Nguyễn (C) sau khi C chạy incremental.

---

## 7. Bàn giao cho người sau

- Nhắn nhóm các version thật từ `state.json`: `silver_initial_version`, `gold_initial_version`, `gold_source_silver_version`, `initial_implementation_sha256`. C (incremental) và E (release) đọc trực tiếp file này.
- Khánh là **steward** trong lệnh release: `--steward <tên Khánh>`.
- Commit: `templates/student_impl.py` (`build_gold`) và `docs/Task2_Tutorial.md`. `evidence/` và `data/` bị `.gitignore`, nên chia sẻ evidence chính thức theo thỏa thuận nhóm.
- Không đưa vào repo/bài nộp: credential, payload thô, `operator_email`, file ZIP của TA.
- Bước tiếp theo trong chuỗi: Task 3 (C) `--step incremental`, dùng **cùng** `run_label` và `evidence_dir` chính thức.

---

## 8. Lỗi thường gặp và cách xử lý

| Triệu chứng | Nguyên nhân thường gặp | Cách xử lý |
|---|---|---|
| `NotImplementedError: TODO 1: build_gold` | Chưa hoàn thành hàm | Làm bước 2; bootstrap dừng **trước** khi ghi bảng |
| `USE_NEW_RUN_LABEL_OR_RESUME_NEXT_STEP` | `state.json` đã có ở `evidence_dir` | Đổi `run_label` **và** `evidence_dir`; không xoá thư mục cũ |
| `CLUSTER_REQUIRES_SHARED_S3A_STORAGE` | Dùng cluster nhưng `lakehouse_root` không phải `s3a://` | Dùng `config/cluster.example.json` đúng hướng dẫn TA |
| `schema_match: False` ở bước 3 | Sai kiểu/thứ tự cột | So `want`/`got` được in ra; kiểm tra cast `decimal(18,2)`, `decimal(18,6)`, `bigint` |
| B03 lệch | `event_date`/`site` NULL, join làm nhân đôi, sai múi giờ | Kiểm tra nhóm NULL; không join QA reference |
| `n_above_25` lệch | Dùng `>=` thay vì `>` | Sửa thành lớn hơn chặt 25 |
| `ModuleNotFoundError: pyspark` | Chưa activate `.venv` | `source .venv/bin/activate` |
| Bootstrap fail giữa chừng | Không có giao dịch đa bảng; có thể còn bảng staging dở | **Không** sửa `_delta_log`/VACUUM; giữ file lỗi, dùng `run_label` mới, hỏi TA nếu cần |
| `task2_verify.py` báo thiếu file | Sai `--evidence` hoặc bootstrap chưa xong | `ls "$EV"`; kiểm tra `evidence_dir` trong `config/team.json` |

---

## 9. Kết quả tham chiếu

Quan sát trên lần chạy thử `khanh-task2-01` (WSL2, `.venv`, Delta-Spark 4.0.1, Python 3.11, dữ liệu CSV fallback của khoá học). Đây là **số tham chiếu để đối chiếu**, không phải kết quả bảo đảm trên mọi máy; `run_ids`, hash code và đường dẫn bảng khác nhau theo lần chạy.

```text
precheck (bước 3):  schema_match True | B03 count 9610 == 9610 | B03 sum 220745.50 == 220745.50 | PASS
bootstrap (bước 4): COMPLETE
bootstrap.json:     rows 9610 | bronze_rows 9610 | gold_rows 16 | silver_version 0 | gold_version 0
state.json:         bronze/silver/gold_initial_version 0 | gold_source_silver_version 0
                    handoff_kind course_fallback_not_student_approval
task2_verify.py:    B01 PASS | 11 cột PASS | B03 PASS (9610; 220745.50) | 16 nhóm Gold = 16 nhóm Silver
                    mọi nhóm khớp tính lại độc lập (0 lệch) | B02 PASS | OVERALL PASS

B04 (trung bình có trọng số vs trung bình của các trung bình site):
day         weighted     unweighted    diff
2026-02-02  22.998594    23.000388     -0.001794
2026-02-03  22.936111    22.936111     +0.000000
2026-02-04  22.950000    22.950000     +0.000000
2026-02-05  23.034533    23.034212     +0.000322
```

Nhận xét B04: hai giá trị khác nhau ở ngày `2026-02-02` (−0,001794) và `2026-02-05` (+0,000322), vì số quan sát `n_i` của các site trong những ngày đó không bằng nhau. Hai ngày còn lại trùng đến 6 chữ số thập phân (có thể do số quan sát của các site bằng nhau; kiểm tra lại bằng `n_observations` trong `gold_baseline.json`). Chênh lệch nhỏ với dữ liệu này, nhưng nguyên lý không đổi: khi `n_i` lệch nhau nhiều thì hai cách tính tách xa nhau. Khi nộp, đối chiếu `(sum_i, n_i)` của từng site ở ngày `2026-02-02` trong `gold_baseline.json`.

> Không sao chép số của máy khác làm kết quả của mình. Đây là lần chạy **thử**; số nộp lấy từ lần chạy chính thức của nhóm (mục 4.2).
