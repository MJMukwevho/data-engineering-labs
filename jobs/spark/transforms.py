"""DataFrame transformations for the FRED pipeline.

Pure functions (DataFrame in, DataFrame out) so they can be unit-tested without
the object store or PostgreSQL. The jobs fred_silver.py and fred_gold.py wire
them to real storage.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DecimalType, StringType, StructField, StructType

# FRED values have at most 4 decimal places (index levels, yields in %)
VALUE_TYPE = DecimalType(18, 4)

# Explicit schema for the raw FRED response: no inference, and unexpected fields are ignored
RAW_SCHEMA = StructType([
    StructField("observation_start", StringType()),
    StructField("observation_end", StringType()),
    StructField("units", StringType()),
    StructField("observations", ArrayType(StructType([
        StructField("realtime_start", StringType()),
        StructField("realtime_end", StringType()),
        StructField("date", StringType()),
        StructField("value", StringType()),
    ]))),
])

# Series that are price indices, so returns and volatility make sense for them.
# VIXCLS (a volatility level) and DGS10 (a yield) are kept in the wide table only.
EQUITY_INDICES = ("SP500", "DJIA", "NASDAQCOM")

# One trading month; annualised with ~252 trading days per year
VOLATILITY_WINDOW = 21
TRADING_DAYS_PER_YEAR = 252


# --- Silver ----------------------------------------------------------------

def parse_raw(raw: DataFrame) -> DataFrame:
    """One row per observation, typed.

    Expects the raw JSON columns plus the partition columns series_id and
    ingest_date that Spark reads from the folder names. FRED uses "." for a
    missing value (e.g. a market holiday); that becomes NULL.
    """
    return (
        raw.select(
            F.col("series_id").cast("string"),
            F.col("ingest_date").cast("date"),
            F.explode("observations").alias("obs"),
        )
        .select(
            "series_id",
            F.to_date("obs.date").alias("observation_date"),
            F.when(F.col("obs.value") == ".", None)
            .otherwise(F.col("obs.value"))
            .cast(VALUE_TYPE)
            .alias("value"),
            "ingest_date",
        )
    )


def keep_latest_ingest(observations: DataFrame) -> DataFrame:
    """Deduplicate overlapping loads: per series and date, keep the most recent ingest.

    FRED sometimes revises recent values, so the latest load wins.
    """
    latest_first = Window.partitionBy("series_id", "observation_date").orderBy(F.col("ingest_date").desc())
    return (
        observations
        .withColumn("_rank", F.row_number().over(latest_first))
        .filter("_rank = 1")
        .drop("_rank")
    )


def silver_quality_issues(silver: DataFrame, today) -> list[str]:
    """Returns a description of each failed check; an empty list means the data is fine."""
    issues = []

    if silver.limit(1).count() == 0:
        return ["silver is empty"]

    duplicates = silver.groupBy("series_id", "observation_date").count().filter("count > 1").count()
    if duplicates:
        issues.append(f"{duplicates} duplicate (series_id, observation_date) pairs")

    bad_dates = silver.filter(F.col("observation_date").isNull()).count()
    if bad_dates:
        issues.append(f"{bad_dates} rows with an unparseable date")

    future = silver.filter(F.col("observation_date") > F.lit(today)).count()
    if future:
        issues.append(f"{future} observations dated after {today}")

    negative = silver.filter(F.col("value") < 0).count()
    if negative:
        issues.append(f"{negative} negative values")

    return issues


# --- Gold ------------------------------------------------------------------

def equity_index_performance(silver: DataFrame) -> DataFrame:
    """Daily return and annualised rolling volatility for each equity index.

    Days without a value (holidays) are dropped first, so each return compares
    consecutive trading days. Volatility is NULL until a full window is available.
    """
    by_date = Window.partitionBy("series_id").orderBy("observation_date")
    trailing = by_date.rowsBetween(-(VOLATILITY_WINDOW - 1), 0)

    trading_days = silver.filter(F.col("series_id").isin(*EQUITY_INDICES) & F.col("value").isNotNull())
    value = F.col("value").cast("double")

    with_returns = trading_days.withColumn("daily_return", value / F.lag(value).over(by_date) - 1)

    return (
        with_returns
        .withColumn("_n", F.count("daily_return").over(trailing))
        .withColumn(
            f"volatility_{VOLATILITY_WINDOW}d_annualized",
            F.when(
                F.col("_n") == VOLATILITY_WINDOW,
                F.stddev_samp("daily_return").over(trailing) * F.sqrt(F.lit(float(TRADING_DAYS_PER_YEAR))),
            ),
        )
        .select(
            "series_id",
            "observation_date",
            "value",
            F.round("daily_return", 6).alias("daily_return"),
            F.round(f"volatility_{VOLATILITY_WINDOW}d_annualized", 6).alias(f"volatility_{VOLATILITY_WINDOW}d_annualized"),
        )
    )


def market_daily(silver: DataFrame) -> DataFrame:
    """One row per date with each series as a column (e.g. sp500, vixcls, dgs10).

    Makes cross-series questions easy, such as how the S&P 500 moved when the
    10-year yield rose.
    """
    series = sorted(r.series_id for r in silver.select("series_id").distinct().collect())
    wide = silver.groupBy("observation_date").pivot("series_id", series).agg(F.first("value"))
    return wide.select(
        "observation_date", *[F.col(s).alias(s.lower()) for s in series]
    ).orderBy("observation_date")
