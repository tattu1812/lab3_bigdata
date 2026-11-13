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
