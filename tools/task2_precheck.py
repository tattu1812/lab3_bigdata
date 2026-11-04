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
