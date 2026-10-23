"""Raw -> silver: typed, deduplicated FRED observations as Parquet.

Reads   s3a://<LAKE_BUCKET>/raw/fred/series_id=*/ingest_date=*/observations.json
Writes  s3a://<LAKE_BUCKET>/silver/fred_observations/   (partitioned by series_id)

Run:
    docker compose exec spark-master spark-submit /opt/spark-jobs/fred_silver.py

The silver table is rebuilt from all raw data on each run, so re-running is safe.
The job fails (exit code 1) if a data quality check does not pass, and nothing is written.
"""

import datetime
import os
import sys

from pyspark.sql import SparkSession

import transforms


def main() -> int:
    bucket = os.environ["LAKE_BUCKET"]
    raw_path = f"s3a://{bucket}/raw/fred/"
    silver_path = f"s3a://{bucket}/silver/fred_observations/"

    spark = SparkSession.builder.appName("fred-silver").getOrCreate()

    raw = (
        spark.read
        .schema(transforms.RAW_SCHEMA)
        .option("multiLine", True)
        # Skip the _metadata.json files that sit next to each observations.json
        .option("pathGlobFilter", "observations.json")
        .json(raw_path)
    )
    silver = transforms.keep_latest_ingest(transforms.parse_raw(raw)).cache()

    issues = transforms.silver_quality_issues(silver, datetime.date.today())
    if issues:
        print("Data quality checks FAILED, nothing written:")
        for issue in issues:
            print(f"  - {issue}")
        spark.stop()
        return 1

    silver.write.mode("overwrite").partitionBy("series_id").parquet(silver_path)

    print(f"Wrote silver to {silver_path}")
    summary = (
        spark.read.parquet(silver_path)
        .groupBy("series_id")
        .agg({"observation_date": "max", "value": "count", "*": "count"})
        .withColumnsRenamed({
            "count(1)": "rows",
            "count(value)": "with_value",
            "max(observation_date)": "latest_date",
        })
        .orderBy("series_id")
    )
    summary.show(truncate=False)

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
