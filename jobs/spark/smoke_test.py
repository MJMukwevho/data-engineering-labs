"""Checks that Spark can reach the object store and PostgreSQL.

Run it with:
    docker compose exec spark-master spark-submit /opt/spark-jobs/smoke_test.py

It writes a tiny DataFrame to s3a://<bucket>/_smoke_test/ and to the table
public.smoke_test in PostgreSQL, reads both back, and exits non-zero on any mismatch.
"""

import os
import sys

from pyspark.sql import SparkSession


def main() -> int:
    spark = SparkSession.builder.appName("stack-smoke-test").getOrCreate()
    df = spark.createDataFrame(
        [("SP500", 1), ("DJIA", 2), ("NASDAQCOM", 3)],
        ["series_id", "n"],
    )
    expected = df.count()
    ok = True

    path = f"s3a://{os.environ['LAKE_BUCKET']}/_smoke_test/"
    df.write.mode("overwrite").parquet(path)
    got = spark.read.parquet(path).count()
    print(f"[object store] wrote {expected} rows to {path}, read back {got}")
    ok &= got == expected

    jdbc_url = f"jdbc:postgresql://postgres:5432/{os.environ['POSTGRES_DB']}"
    props = {
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
        "driver": "org.postgresql.Driver",
    }
    df.write.jdbc(jdbc_url, "public.smoke_test", mode="overwrite", properties=props)
    got = spark.read.jdbc(jdbc_url, "public.smoke_test", properties=props).count()
    print(f"[postgres]     wrote {expected} rows to public.smoke_test, read back {got}")
    ok &= got == expected

    spark.stop()
    print("SMOKE TEST PASSED" if ok else "SMOKE TEST FAILED")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
