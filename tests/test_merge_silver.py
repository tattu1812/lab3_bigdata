"""Delta MERGE tests, confined to a new dedicated fixture root in WSL."""
import json
import os
import sys
import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from tempfile import gettempdir

from delta.tables import DeltaTable
from pyspark.sql import functions as F, types as T

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
sys.path.insert(0, str(ROOT / 'templates'))
from session import make_spark
from student_impl import choose_revisions, merge_silver

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


def record(key='R000001', minute=20, obj='b.csv', ordinal=5,
           temp='20.00', digest='a'):
    return {
        'record_id': key, 'sensor_id': 'S01', 'site': 'zone-1',
        'event_time_utc': datetime(2026, 2, 6, 0, 0, tzinfo=timezone.utc),
        'ingest_time_utc': datetime(2026, 2, 6, 0, minute, tzinfo=timezone.utc),
        'temperature_c': Decimal(temp), 'is_late': minute > 15,
        'source_object': obj, 'source_row': ordinal, 'source_sha256': digest * 64,
        'event_date': date(2026, 2, 6),
    }


class MergeSilverTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # No fallback to a baseline path, and never reuse or remove old fixtures.
        raw = os.environ.get('LAB3_MERGE_TEST_ROOT')
        if not raw:
            raise RuntimeError('LAB3_MERGE_TEST_ROOT is required')
        temp_base = Path(gettempdir()).resolve()
        allowed = (temp_base / 'lab3-merge-tests').resolve()
        if temp_base not in allowed.parents:
            raise RuntimeError('Dedicated test directory must remain within the system temporary directory')
        requested = Path(raw).expanduser()
        if not requested.is_absolute():
            raise RuntimeError('LAB3_MERGE_TEST_ROOT must be an absolute path')
        cls.test_root = requested.resolve()
        if cls.test_root.parent != allowed:
            raise RuntimeError('Test root must be a new direct child of the dedicated temporary test directory')

        # Refuse to place fixture data in the repository or any configured lakehouse.
        protected = [ROOT.resolve()]
        config_file = ROOT / 'config/team.json'
        if config_file.is_file():
            config = json.loads(config_file.read_text(encoding='utf-8'))
            lake = config.get('lakehouse_root', '')
            if lake.startswith('/'):
                protected.append(Path(lake).resolve())
            state_file = ROOT / config.get('evidence_dir', 'evidence') / 'state.json'
            if state_file.is_file():
                state = json.loads(state_file.read_text(encoding='utf-8'))
                paths = [state.get(name + '_path', '') for name in ('bronze', 'silver', 'gold')]
                paths.append(state.get('baseline_copy_provenance', {}).get('source_tables', ''))
                protected.extend(Path(path).resolve() for path in paths if path.startswith('/'))
        if any(cls.test_root == path or path in cls.test_root.parents for path in protected):
            raise RuntimeError('Test root overlaps a protected repository or lakehouse path')
        cls.test_root.mkdir(parents=True, exist_ok=False)
        print('ISOLATED_DELTA_TEST_ROOT:', cls.test_root, flush=True)
        cls.spark = make_spark({
            'group_id': 'step5-unit-tests', 'spark_master': 'local[2]',
            'lakehouse_root': str(cls.test_root), 'shuffle_partitions': 2,
            'extra_maven_packages': [],
        })
        cls.spark.sparkContext.setLogLevel('ERROR')

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        # Fixtures deliberately remain on disk; no cleanup, reset or VACUUM.

    def frame(self, rows):
        return self.spark.createDataFrame(rows, SCHEMA)

    def table(self, rows):
        path = str(self.test_root / self._testMethodName)
        self.frame(rows).write.format('delta').mode('errorifexists').save(path)
        return path

    def read(self, path):
        return self.spark.read.format('delta').load(path)

    def canonical(self, df):
        return sorted((r.asDict(recursive=True) for r in df.collect()),
                      key=lambda row: row['record_id'])

    def assert_matches(self, path, expected):
        self.assertEqual(self.canonical(self.read(path)), self.canonical(self.frame(expected)))

    def test_01_insert_new_key_once(self):
        old = record(key='R000001')
        new = record(key='R000002', ordinal=9, temp='23.45', digest='b')
        path = self.table([old])
        self.assertIsNone(merge_silver(self.spark, path, self.frame([new])))
        self.assert_matches(path, [old, new])

    def test_02_newer_ingest_updates(self):
        old = record(minute=10, obj='a.csv', ordinal=1, temp='18.10')
        new = record(minute=30, obj='z.csv', ordinal=99, temp='22.25', digest='f')
        path = self.table([old])
        merge_silver(self.spark, path, self.frame([new]))
        self.assert_matches(path, [new])

    def test_03_stale_does_not_update(self):
        old = record(minute=30, obj='z.csv', ordinal=99, temp='22.25')
        stale = record(minute=10, obj='a.csv', ordinal=1, temp='18.10', digest='f')
        path = self.table([old])
        merge_silver(self.spark, path, self.frame([stale]))
        self.assert_matches(path, [old])

    def test_04_equal_time_smaller_object_wins(self):
        old = record(minute=20, obj='b.csv', ordinal=2)
        new = record(minute=20, obj='a.csv', ordinal=99, temp='22.10', digest='b')
        path = self.table([old])
        merge_silver(self.spark, path, self.frame([new]))
        self.assert_matches(path, [new])

    def test_05_equal_time_and_object_smaller_row_wins(self):
        old = record(ordinal=10)
        new = record(ordinal=2, temp='22.20', digest='b')
        path = self.table([old])
        merge_silver(self.spark, path, self.frame([new]))
        self.assert_matches(path, [new])

    def test_06_equal_priority_is_noop_even_if_payload_differs(self):
        old = record(ordinal=5, temp='18.10', digest='a')
        equal = record(ordinal=5, temp='29.90', digest='f')
        path = self.table([old])
        merge_silver(self.spark, path, self.frame([equal]))
        self.assert_matches(path, [old])

    def test_07_mixed_insert_update_and_noop(self):
        old = [record(key='R000001', minute=10), record(key='R000002', minute=30),
               record(key='R000003', minute=20)]
        incoming = [record(key='R000001', minute=30, temp='21.11', digest='b'),
                    record(key='R000002', minute=10, temp='22.22', digest='c'),
                    record(key='R000003', minute=20, temp='23.33', digest='d'),
                    record(key='R000004', minute=20, temp='24.44', digest='e')]
        path = self.table(old)
        merge_silver(self.spark, path, self.frame(incoming))
        self.assert_matches(path, [incoming[0], old[1], old[2], incoming[3]])
        print('MIXED_FIXTURE_EVIDENCE insert=1 update=1 stale_noop=1 equal_noop=1', flush=True)

    def test_08_replay_keeps_canonical_rows_and_provenance(self):
        old = record(key='R000001', minute=10)
        batch = [record(key='R000001', minute=30, temp='22.01', digest='b'),
                 record(key='R000002', minute=20, temp='23.02', digest='c')]
        path = self.table([old])
        winners = self.frame(batch)
        merge_silver(self.spark, path, winners)
        first = self.canonical(self.read(path))
        version1 = DeltaTable.forPath(self.spark, path).history(1).select('version').first()[0]
        merge_silver(self.spark, path, winners)
        second = self.canonical(self.read(path))
        version2 = DeltaTable.forPath(self.spark, path).history(1).select('version').first()[0]
        self.assertEqual(first, second)
        self.assertEqual(len(first), 2)
        self.assert_matches(path, batch)
        print('REPLAY_FIXTURE_EVIDENCE first_version=%s replay_version=%s rows=2 canonical_equal=True' % (version1, version2), flush=True)

    def test_09_schema_and_provenance_are_preserved(self):
        old = record(minute=10)
        new = record(minute=20, obj='source-new.csv', ordinal=123, digest='f')
        path = self.table([old])
        before_schema = self.read(path).schema
        merge_silver(self.spark, path, self.frame([new]))
        after = self.read(path)
        self.assertEqual(after.schema, before_schema)
        contract = json.loads((ROOT / 'schemas/silver_schema.json').read_text())['columns']
        self.assertEqual([(f.name, f.dataType.simpleString()) for f in after.schema],
                         [(c['name'], c['type'].lower()) for c in contract])
        row = after.first()
        self.assertEqual((row.source_object, row.source_row, row.source_sha256),
                         ('source-new.csv', 123, 'f' * 64))

    def test_10_utc_date_and_exact_decimal(self):
        old = record(minute=10)
        old.update(event_time_utc=datetime(2026, 2, 6, 23, 59, 30, tzinfo=timezone.utc),
                   ingest_time_utc=datetime(2026, 2, 7, 0, 1, tzinfo=timezone.utc),
                   event_date=date(2026, 2, 6), is_late=False)
        new = dict(old, ingest_time_utc=datetime(2026, 2, 7, 0, 31, tzinfo=timezone.utc),
                   temperature_c=Decimal('-0.10'), is_late=True, source_row=6, source_sha256='b' * 64)
        path = self.table([old])
        merge_silver(self.spark, path, self.frame([new]))
        result = self.read(path)
        row = result.select('event_date', 'temperature_c',
                            F.date_format('event_time_utc', 'yyyy-MM-dd HH:mm:ss').alias('utc_event')).first()
        self.assertEqual(row.event_date, date(2026, 2, 6))
        self.assertEqual(row.utc_event, '2026-02-06 23:59:30')
        self.assertEqual(row.temperature_c, Decimal('-0.10'))
        self.assertEqual(result.schema['temperature_c'].dataType, T.DecimalType(8, 2))

    def test_11_unique_target_after_reducing_source(self):
        old = record(key='R000001', minute=10)
        incoming = [record(key='R000001', minute=15, ordinal=2),
                    record(key='R000001', minute=30, ordinal=3, digest='b'),
                    record(key='R000002', minute=20, ordinal=4, digest='c')]
        path = self.table([old])
        winners = choose_revisions(self.frame(incoming))
        self.assertEqual(winners.count(), 2)
        merge_silver(self.spark, path, winners)
        target = self.read(path)
        self.assertEqual(target.groupBy('record_id').count().where(F.col('count') > 1).count(), 0)
        self.assert_matches(path, [incoming[1], incoming[2]])

    def test_12_strict_priority_matrix(self):
        old = [record(key='R%06d' % i, minute=20, obj='b.csv', ordinal=5,
                      temp='10.00', digest='a') for i in range(10, 17)]
        incoming = [
            record(key='R000010', minute=30, obj='z.csv', ordinal=99, temp='21.00', digest='b'),
            record(key='R000011', minute=10, obj='a.csv', ordinal=1, temp='21.01', digest='b'),
            record(key='R000012', minute=20, obj='a.csv', ordinal=99, temp='21.02', digest='b'),
            record(key='R000013', minute=20, obj='z.csv', ordinal=1, temp='21.03', digest='b'),
            record(key='R000014', minute=20, obj='b.csv', ordinal=2, temp='21.04', digest='b'),
            record(key='R000015', minute=20, obj='b.csv', ordinal=8, temp='21.05', digest='b'),
            record(key='R000016', minute=20, obj='b.csv', ordinal=5, temp='21.06', digest='b'),
        ]
        expected = [incoming[0], old[1], incoming[2], old[3], incoming[4], old[5], old[6]]
        path = self.table(old)
        merge_silver(self.spark, path, self.frame(incoming))
        self.assert_matches(path, expected)
        print('STRICT_MATRIX_EVIDENCE newer/update older/noop object_less/update object_greater/noop row_less/update row_greater/noop equal/noop', flush=True)

    def test_13_combined_choose_then_merge(self):
        old = [record(key='R000001', minute=10), record(key='R000002', minute=30)]
        incoming = [record(key='R000001', minute=30, obj='z.csv', ordinal=9, temp='23.00'),
                    record(key='R000001', minute=30, obj='a.csv', ordinal=8, temp='22.00', digest='b'),
                    record(key='R000002', minute=20, obj='a.csv', ordinal=3, temp='18.00', digest='c'),
                    record(key='R000003', minute=20, obj='a.csv', ordinal=4, temp='24.00', digest='d')]
        path = self.table(old)
        winners = choose_revisions(self.frame(incoming))
        self.assertEqual(winners.count(), 3)
        merge_silver(self.spark, path, winners)
        self.assert_matches(path, [incoming[1], old[1], incoming[3]])
        print('COMBINED_FIXTURE_EVIDENCE physical=4 winners=3 insert=1 update=1 stale_noop=1', flush=True)


if __name__ == '__main__':
    unittest.main(verbosity=2)
