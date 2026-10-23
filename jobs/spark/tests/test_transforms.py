"""Unit tests for transforms.py, using a local Spark session (no storage needed).

Run:
    docker compose exec spark-master python3 -m unittest discover -s /opt/spark-jobs/tests -v
"""

import datetime
import os
import sys
import unittest
import warnings
from decimal import Decimal

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pyspark.sql import Row, SparkSession  # noqa: E402
from pyspark.sql.types import StringType, StructField, StructType  # noqa: E402

import transforms  # noqa: E402

D = datetime.date


def obs(date, value):
    return Row(realtime_start=None, realtime_end=None, date=date, value=value)


class TransformsTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # PySpark's Java bridge leaves sockets for the JVM to close; the warnings are noise
        warnings.simplefilter("ignore", ResourceWarning)
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("transforms-test")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def raw(self, rows):
        """rows: (series_id, ingest_date, [obs, ...]), shaped like the raw JSON plus partition columns"""
        schema = StructType(
            transforms.RAW_SCHEMA.fields
            + [StructField("series_id", StringType()), StructField("ingest_date", StringType())]
        )
        return self.spark.createDataFrame([(None, None, None, o, s, i) for s, i, o in rows], schema)

    def silver(self, rows):
        """rows: (series_id, date, value)"""
        return self.spark.createDataFrame(
            [Row(series_id=s, observation_date=d, value=None if v is None else Decimal(str(v))) for s, d, v in rows],
            "series_id string, observation_date date, value decimal(18,4)",
        )

    # --- silver --------------------------------------------------------------

    def test_parse_raw_types_values_and_missing_marker(self):
        raw = self.raw([("SP500", "2026-10-22", [obs("2026-10-20", "6512.30"), obs("2026-10-21", ".")])])

        rows = transforms.parse_raw(raw).orderBy("observation_date").collect()

        self.assertEqual(
            [(r.series_id, r.observation_date, r.value, r.ingest_date) for r in rows],
            [
                ("SP500", D(2026, 10, 20), Decimal("6512.3000"), D(2026, 10, 22)),
                ("SP500", D(2026, 10, 21), None, D(2026, 10, 22)),
            ],
        )

    def test_keep_latest_ingest_prefers_revised_value(self):
        raw = self.raw([
            ("DGS10", "2026-10-21", [obs("2026-10-20", "4.10"), obs("2026-10-21", "4.12")]),
            ("DGS10", "2026-10-22", [obs("2026-10-21", "4.15"), obs("2026-10-22", "4.20")]),
        ])

        rows = transforms.keep_latest_ingest(transforms.parse_raw(raw)).orderBy("observation_date").collect()

        self.assertEqual(
            [(r.observation_date, r.value) for r in rows],
            [(D(2026, 10, 20), Decimal("4.1000")), (D(2026, 10, 21), Decimal("4.1500")), (D(2026, 10, 22), Decimal("4.2000"))],
        )

    def test_quality_checks_pass_on_clean_data(self):
        silver = self.silver([("SP500", D(2026, 10, 20), 6500), ("SP500", D(2026, 10, 21), None)])

        self.assertEqual(transforms.silver_quality_issues(silver, D(2026, 10, 22)), [])

    def test_quality_checks_catch_problems(self):
        silver = self.silver([
            ("SP500", D(2026, 10, 20), 6500),
            ("SP500", D(2026, 10, 20), 6501),   # duplicate
            ("SP500", D(2026, 12, 1), 6600),    # future
            ("DGS10", D(2026, 10, 20), -1),     # negative
        ])

        issues = transforms.silver_quality_issues(silver, D(2026, 10, 22))

        self.assertEqual(len(issues), 3, issues)
        self.assertIn("duplicate", issues[0])
        self.assertIn("after 2026-10-22", issues[1])
        self.assertIn("negative", issues[2])

    def test_quality_checks_flag_empty_silver(self):
        self.assertEqual(transforms.silver_quality_issues(self.silver([]), D(2026, 10, 22)), ["silver is empty"])

    # --- gold ----------------------------------------------------------------

    def test_daily_return_skips_holidays_and_non_equity_series(self):
        silver = self.silver([
            ("SP500", D(2026, 10, 19), 100),
            ("SP500", D(2026, 10, 20), None),   # holiday
            ("SP500", D(2026, 10, 21), 110),
            ("SP500", D(2026, 10, 22), 99),
            ("VIXCLS", D(2026, 10, 21), 15),    # not an equity index
        ])

        rows = transforms.equity_index_performance(silver).orderBy("observation_date").collect()

        self.assertEqual(
            [(r.series_id, r.observation_date, r.daily_return) for r in rows],
            [("SP500", D(2026, 10, 19), None), ("SP500", D(2026, 10, 21), 0.1), ("SP500", D(2026, 10, 22), -0.1)],
        )

    def test_volatility_needs_a_full_window(self):
        window = transforms.VOLATILITY_WINDOW
        start = D(2026, 1, 1)
        # Alternating +1% / -1% moves: one more value than returns, so the last row has a full window
        values, v = [], 100.0
        for i in range(window + 1):
            values.append(v)
            v *= 1.01 if i % 2 == 0 else 0.99
        silver = self.silver([("DJIA", start + datetime.timedelta(days=i), round(x, 4)) for i, x in enumerate(values)])

        rows = transforms.equity_index_performance(silver).orderBy("observation_date").collect()
        column = f"volatility_{window}d_annualized"

        self.assertTrue(all(r[column] is None for r in rows[:-1]))
        self.assertAlmostEqual(rows[-1][column], 0.01 * (252 ** 0.5), delta=0.01)

    def test_market_daily_pivots_series_into_columns(self):
        silver = self.silver([
            ("SP500", D(2026, 10, 21), 6500),
            ("DGS10", D(2026, 10, 21), 4.12),
            ("SP500", D(2026, 10, 22), 6510),
        ])

        df = transforms.market_daily(silver)
        rows = df.collect()

        self.assertEqual(df.columns, ["observation_date", "dgs10", "sp500"])
        self.assertEqual(
            [(r.observation_date, r.dgs10, r.sp500) for r in rows],
            [(D(2026, 10, 21), Decimal("4.1200"), Decimal("6500.0000")), (D(2026, 10, 22), None, Decimal("6510.0000"))],
        )


if __name__ == "__main__":
    unittest.main()
