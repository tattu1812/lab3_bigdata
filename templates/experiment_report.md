# Lab 3 experiment report
Group / run label / author / reviewer:

## 1. Upstream handoff
State whether you used your approved Lab 2 Parquet or the course fallback.
Record the independently supplied manifest hash, source release ID, schema,
AS_OF, and logical baseline hash. Do not claim the course fixture was signed
or published by your group. Explain where the original Lab 2 raw data remain.

## 2. Table semantics and lineage
Record Bronze, Silver and Gold URIs and table versions. Explain a Bronze
receipt, a Silver business-key winner and a Gold site/day aggregation.
Trace one updated row to source object, physical row and SHA-256.
Draw only actual read/write edges; governance metadata are not data carriers.

## 3. Incremental evidence
Report incoming physical records, per-key winners, inserts, updates,
unchanged/stale records, Bronze count and Silver count. Show the replay
and time-travel results. Distinguish logical equivalence from Delta version
identity and Parquet file-byte identity. Explain the invalid-batch policy.

## 4. Analytics
The analytics were generated from Gold version 1, which was rebuilt from
Silver version 1. The saved outputs are
`evidence/task04-Hanh/Q1_site_day.json`,
`evidence/task04-Hanh/Q2_weighted_day.json`, and
`evidence/task04-Hanh/Q3_above25_rate.json`.

Q1 contains 20 site/day rows covering four sites and five UTC event dates.
The daily late-arrival numerators and denominators were:

| UTC date | n_late | n_observations | late fraction |
|---|---:|---:|---:|
| 2026-02-02 | 200 | 2,489 | 0.080354 |
| 2026-02-03 | 0 | 2,880 | 0.000000 |
| 2026-02-04 | 0 | 2,880 | 0.000000 |
| 2026-02-05 | 0 | 1,361 | 0.000000 |
| 2026-02-06 | 20 | 100 | 0.200000 |

Q3 used the strict teaching threshold `temperature_c > 25`. Its daily
numerators and denominators were:

| UTC date | n_above_25 | n_observations | above-25 fraction |
|---|---:|---:|---:|
| 2026-02-02 | 739 | 2,489 | 0.296906 |
| 2026-02-03 | 832 | 2,880 | 0.288889 |
| 2026-02-04 | 841 | 2,880 | 0.292014 |
| 2026-02-05 | 406 | 1,361 | 0.298310 |
| 2026-02-06 | 0 | 100 | 0.000000 |

Q2 compares observation-weighted daily means with the naive mean of site
means:

| UTC date | weighted mean (C) | naive mean of means (C) |
|---|---:|---:|
| 2026-02-02 | 23.014665 | 23.016459 |
| 2026-02-03 | 22.936111 | 22.936111 |
| 2026-02-04 | 22.950000 | 22.950000 |
| 2026-02-05 | 23.034533 | 23.034212 |
| 2026-02-06 | 21.450000 | 21.450000 |

The weighted mean is computed as `SUM(sum_temperature_c) /
SUM(n_observations)`. It gives each retained observation equal weight.
`AVG(mean_temperature_c)` gives each site/day group equal weight, so the two
values generally differ when site/day groups have different observation
counts. The saved plots are `daily_mean.png` and `late_fraction.png`.

The above-25 fraction is a teaching indicator, not a calibrated physical
safety threshold. The inherited QA reference is not used to replace released
measurements; residual disagreement with that reference remains a known
quality limitation.

## 5. Controlled layout experiment
The experiment used Silver version 1, copy factor 100, and 9,710 source rows,
for 971,000 expanded synthetic rows. It used `local[2]`, AQE enabled and
eight shuffle partitions. Setup time was excluded from query timings:

| Layout | Physical layout | Setup seconds | Total data files | Total bytes |
|---|---|---:|---:|---:|
| A | Unpartitioned Delta, 32 writer partitions | 8.849470 | 32 | 16,396,223 |
| B | Delta partitioned by `event_date`, 5 writer partitions | 3.034906 | 5 | 525,397 |

Both layouts evaluated the same query: filter `event_date =
DATE '2026-02-06'`, group by `site`, and return `site`,
`n_observations`, `sum_temperature_c`, and `n_late`. All six query results
were equal.

The measured order was A-B, B-A, A-B:

| Round | Layout A (seconds) | Layout B (seconds) |
|---|---:|---:|
| 1 | 0.458930 | 0.382030 |
| 2 | 0.418276 | 0.425917 |
| 3 | 0.348516 | 0.278070 |

The medians were 0.418276 seconds for A and 0.382030 seconds for B. The
ratio `median(A) / median(B)` was 1.094880, so A's median was about 9.49%
higher than B's in this repeated/warmed run. This is a bounded synthetic
experiment, not a production-scale speed claim.

The A plan shows a Parquet scan with pushed filters for `event_date`, but A
has no partition columns. The B plan contains
`PartitionFilters: [isnotnull(event_date), (event_date = 2026-02-06)]`,
which is the expected pruning evidence for the date-partitioned layout.
The file counts above are total table metadata, not files actually scanned.
`clearCache()` does not reset operating-system or object-store caches, so the
measurements are not cold-cache timings. The replica-expanded tables remain
in the benchmark area and were not written into operational Gold.

## 6. Release and accountability
Record the pinned Silver/Gold pair and generating-code hash; validator code
may have its own hash. Attach five release-gate outcomes and the owner's
independent result. List who authored/reviewed each technical contribution.
Do not include credentials, kubeconfig, or restricted raw payloads.
