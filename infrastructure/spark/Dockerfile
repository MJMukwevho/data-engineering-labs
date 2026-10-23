# Spark 3.5 matches AWS Glue 5 and EMR 7, so jobs written here carry over to AWS.
FROM apache/spark:3.5.9-scala2.12-java17-python3-ubuntu

# Connectors baked into the image (no downloads when a job starts):
#   hadoop-aws + AWS SDK bundle -> s3a:// access to the object store (versions must match Spark's Hadoop 3.3.4)
#   PostgreSQL JDBC driver      -> read/write the warehouse
ARG MAVEN_REPO=https://repo.maven.apache.org/maven2
USER root
# Downloaded one at a time with retries, so a busy mirror (HTTP 429) doesn't fail the build
RUN set -eu; \
    for jar in \
        org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
        com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
        org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar; \
    do \
        curl -fsSL --retry 5 --retry-all-errors --retry-delay 3 \
             -o "/opt/spark/jars/$(basename "$jar")" "${MAVEN_REPO}/${jar}"; \
    done; \
    chown spark:spark /opt/spark/jars/hadoop-aws-*.jar /opt/spark/jars/aws-java-sdk-bundle-*.jar /opt/spark/jars/postgresql-*.jar

COPY spark-defaults.conf log4j2.properties /opt/spark/conf/
USER spark

ENV PATH=/opt/spark/bin:$PATH
# Lets plain `python3` import PySpark (used by the unit tests); spark-submit doesn't need this
ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip
ENV PYTHONDONTWRITEBYTECODE=1
