"""Silver -> gold: analysis-ready tables in the lake and in PostgreSQL.

Reads   s3a://<LAKE_BUCKET>/silver/fred_observations/
Writes  for each gold table:
          s3a://<LAKE_BUCKET>/gold/<table>/   (Parquet, for Athena-style queries)
          PostgreSQL gold.<table>             (for BI tools and SQL clients)

Tables
  equity_index_performance  daily return and 21-day annualised volatility per index
  market_daily              one row per date, one column per series

Run after fred_silver.py:
    docker compose exec spark-master spark-submit /opt/spark-jobs/fred_gold.py
"""

import os
import sys

from pyspark.sql import DataFrame, SparkSession

import transforms


def write_table(df: DataFrame, name: str, bucket: str) -> None:
    lake_path = f"s3a://{bucket}/gold/{name}/"
    df.write.mode("overwrite").parquet(lake_path)

    jdbc_url = f"jdbc:postgresql://postgres:5432/{os.environ['POSTGRES_DB']}"
    (
        df.write.mode("overwrite")
        # Empty the existing table instead of dropping it, so grants and views on it survive
        .option("truncate", "true")
        .jdbc(
            jdbc_url,
            f"gold.{name}",
            properties={
                "user": os.environ["POSTGRES_USER"],
                "password": os.environ["POSTGRES_PASSWORD"],
                "driver": "org.postgresql.Driver",
            },
        )
    )
    print(f"Wrote gold.{name}: {df.count()} rows -> {lake_path} and PostgreSQL")


def main() -> int:
    bucket = os.environ["LAKE_BUCKET"]
    spark = SparkSession.builder.appName("fred-gold").getOrCreate()

    silver = spark.read.parquet(f"s3a://{bucket}/silver/fred_observations/").cache()
    if silver.limit(1).count() == 0:
        print("Silver is empty: run fred_silver.py first")
        spark.stop()
        return 1

    performance = transforms.equity_index_performance(silver).cache()
    if performance.limit(1).count() == 0:
        print(f"No equity index data found (expected one of {transforms.EQUITY_INDICES})")
        spark.stop()
        return 1

    write_table(performance, "equity_index_performance", bucket)
    write_table(transforms.market_daily(silver), "market_daily", bucket)

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
