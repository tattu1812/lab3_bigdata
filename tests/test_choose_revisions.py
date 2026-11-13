"""Read-only Spark unit tests for Task 3 choose_revisions."""
import os
import sys
import time
import unittest
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
sys.path.insert(0, str(ROOT / 'templates'))

from common import as_time, load_csv, load_registry, validate_rows, verify_inputs
from run_lab import check_provenance, transport_df
from student_impl import choose_revisions


SCHEMA = T.StructType([
    T.StructField('record_id', T.StringType(), False),
    T.StructField('sensor_id', T.StringType(), False),
    T.StructField('site', T.StringType(), False),
    T.StructField('event_time_utc', T.TimestampType(), False),
    T.StructField('ingest_time_utc', T.TimestampType(), False),
    T.StructField('temperature_c', T.DecimalType(8, 2), False),
    T.StructField('is_late', T.BooleanType(), False),
    T.StructField('source_object', T.StringType(), False),
    T.StructField('source_row', T.LongType(), False),
    T.StructField('source_sha256', T.StringType(), False),
    T.StructField('event_date', T.DateType(), False),
])


def fixture(record_id='R000001', minute=10, source='a.csv', row=1,
            temperature='20.00', digest='a'):
    return {
        'record_id': record_id, 'sensor_id': 'S01', 'site': 'zone-1',
        'event_time_utc': datetime(2026, 2, 6, 0, 0, tzinfo=timezone.utc),
        'ingest_time_utc': datetime(2026, 2, 6, 0, minute, tzinfo=timezone.utc),
        'temperature_c': Decimal(temperature), 'is_late': False,
        'source_object': source, 'source_row': row, 'source_sha256': digest * 64,
        'event_date': date(2026, 2, 6),
    }


def independent_winners(rows):
    """Stdlib oracle: explicit three-stage stable sort, no Spark Window."""
    groups = defaultdict(list)
    for row in rows:
        groups[row['record_id']].append(row)
    expected = []
    for group in groups.values():
        ordered = sorted(group, key=lambda x: x['source_row'])
        ordered.sort(key=lambda x: x['source_object'])
        ordered.sort(key=lambda x: as_time(x['ingest_time_utc']), reverse=True)
        expected.append(ordered[0])
    return sorted(expected, key=lambda x: x['record_id'])


class ChooseRevisionsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Match session.make_spark: PySpark collects timestamps using process TZ.
        os.environ['TZ'] = 'UTC'
        if hasattr(time, 'tzset'):
            time.tzset()
        cls.spark = (SparkSession.builder.master('local[2]')
                     .appName('Lab3-Task3-ChooseRevisions-UnitTests')
                     .config('spark.sql.session.timeZone', 'UTC')
                     .config('spark.sql.shuffle.partitions', '4')
                     .config('spark.ui.enabled', 'false')
                     .config('spark.ui.showConsoleProgress', 'false')
                     .getOrCreate())
        cls.spark.sparkContext.setLogLevel('ERROR')
        verified = verify_inputs(ROOT, ROOT / 'trusted_lab3_inputs.json')
        if not all(x['passed'] for x in verified['checks']):
            raise AssertionError('Trusted input verification failed')
        cls.registry = load_registry(ROOT / 'data/handoff/sensors.csv')
        cls.incoming = validate_rows(load_csv(ROOT / 'data/changes/changes_batch02.csv'),
                                     cls.registry, False)
        check_provenance(cls.incoming, ROOT)
        cls.official = transport_df(cls.spark, cls.incoming)
        cls.oracle = independent_winners(cls.incoming)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def frame(self, rows):
        return self.spark.createDataFrame(rows, SCHEMA)

    def canonical_official(self, df):
        rows = [row.asDict(recursive=True) for row in df.collect()]
        return sorted(validate_rows(rows, self.registry, True, allow_event_date=True),
                      key=lambda x: x['record_id'])

    def test_01_single_revision_is_unchanged(self):
        original = self.frame([fixture()])
        result = choose_revisions(original)
        self.assertIsInstance(result, DataFrame)
        self.assertEqual(result.collect(), original.collect())

    def test_02_latest_ingest_wins_before_other_keys(self):
        df = self.frame([fixture(minute=5, source='a.csv', row=1, temperature='18.00'),
                         fixture(minute=30, source='z.csv', row=99, temperature='23.00')])
        winner = choose_revisions(df).first()
        self.assertEqual((winner.source_object, winner.source_row, winner.temperature_c),
                         ('z.csv', 99, Decimal('23.00')))

    def test_03_equal_time_uses_smallest_source_object(self):
        df = self.frame([fixture(source='z.csv', row=1, digest='a'),
                         fixture(source='a.csv', row=99, digest='f')])
        winner = choose_revisions(df).first()
        self.assertEqual((winner.source_object, winner.source_row, winner.source_sha256),
                         ('a.csv', 99, 'f' * 64))

    def test_04_equal_time_and_object_use_numeric_smallest_row(self):
        df = self.frame([fixture(row=10, temperature='21.00', digest='a'),
                         fixture(row=2, temperature='19.00', digest='f')])
        winner = choose_revisions(df).first()
        self.assertEqual((winner.source_row, winner.temperature_c, winner.source_sha256),
                         (2, Decimal('19.00'), 'f' * 64))

    def test_05_reversed_input_keeps_all_values_and_provenance(self):
        rows = [fixture(row=8, temperature='21.00', digest='a'),
                fixture(row=2, temperature='19.00', digest='f'),
                fixture(record_id='R000002', minute=20, row=9, digest='b'),
                fixture(record_id='R000002', minute=10, row=3, digest='c')]
        forward = sorted(choose_revisions(self.frame(rows)).collect(), key=lambda r: r.record_id)
        backward = sorted(choose_revisions(self.frame(list(reversed(rows)))).collect(),
                          key=lambda r: r.record_id)
        self.assertEqual(forward, backward)
        self.assertEqual([(r.record_id, r.source_row) for r in forward],
                         [('R000001', 2), ('R000002', 9)])

    def test_06_schema_provenance_and_helper_name_collision(self):
        df = (self.frame([fixture(row=8), fixture(row=2, digest='f')])
              .withColumn('__lab3_revision_rank', F.lit('preserve'))
              .withColumn('_rn', F.lit(7).cast('long')))
        result = choose_revisions(df)
        self.assertEqual(result.schema, df.schema)
        self.assertEqual(result.columns, df.columns)
        winner = result.first()
        self.assertEqual((winner.source_row, winner.source_sha256,
                          winner['__lab3_revision_rank'], winner['_rn']),
                         (2, 'f' * 64, 'preserve', 7))

    def test_07_official_data_matches_independent_oracle_and_all_ties(self):
        result = choose_revisions(self.official)
        self.assertEqual(result.schema, self.official.schema)
        self.assertEqual(result.columns, self.official.columns)
        actual = self.canonical_official(result)
        self.assertEqual(len(self.incoming), 140)
        self.assertEqual(len(actual), 130)
        self.assertEqual(len({r['record_id'] for r in actual}), 130)
        self.assertEqual(len(self.incoming) - len(actual), 10)
        self.assertEqual(actual, self.oracle)
        groups = defaultdict(list)
        for row in self.incoming:
            groups[row['record_id']].append(row)
        ties = [group for group in groups.values() if len(group) > 1]
        self.assertEqual(Counter(map(len, groups.values())), {1: 120, 2: 10})
        self.assertEqual(len(ties), 10)
        actual_by_id = {row['record_id']: row for row in actual}
        for group in ties:
            self.assertEqual(len({r['ingest_time_utc'] for r in group}), 1)
            self.assertEqual(len({r['source_object'] for r in group}), 1)
            winner = actual_by_id[group[0]['record_id']]
            self.assertEqual(winner['source_row'], min(r['source_row'] for r in group))
            self.assertIn(winner, group)  # Includes every provenance/value field.
        print('OFFICIAL_EVIDENCE: input=140 winners=130 distinct=130 losers=10 row_ties=10')

    def test_08_official_repartition_and_reorder_are_deterministic(self):
        one = self.canonical_official(choose_revisions(self.official.repartition(1)))
        reversed_three = self.canonical_official(choose_revisions(
            self.official.orderBy(F.col('source_row').desc()).repartition(3)))
        seven = self.canonical_official(choose_revisions(self.official.repartition(7, 'record_id')))
        self.assertEqual(one, self.oracle)
        self.assertEqual(reversed_three, self.oracle)
        self.assertEqual(seven, self.oracle)
        print('DETERMINISM_EVIDENCE: partitions=1,3,7; reversed input; full canonical rows equal')

    def test_09_identical_priority_is_reported_as_ambiguous(self):
        df = self.frame([fixture(row=2, temperature='19.00', digest='a'),
                         fixture(row=2, temperature='21.00', digest='f')])
        with self.assertRaisesRegex(ValueError, 'AMBIGUOUS_REVISION_ORDER'):
            choose_revisions(df)

    def test_10_empty_dataframe_keeps_schema(self):
        df = self.frame([])
        result = choose_revisions(df)
        self.assertEqual(result.schema, df.schema)
        self.assertEqual(result.count(), 0)


if __name__ == '__main__':
    unittest.main(verbosity=2)
