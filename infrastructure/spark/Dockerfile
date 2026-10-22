# Spark 3.5 matches AWS Glue 5 and EMR 7, so jobs written here carry over to AWS.
FROM apache/spark:3.5.9-scala2.12-java17-python3-ubuntu

# Connectors baked into the image (no downloads when a job starts):
#   hadoop-aws + AWS SDK bundle -> s3a:// access to the object store (versions must match Spark's Hadoop 3.3.4)
#   PostgreSQL JDBC driver      -> read/write the warehouse
ARG MAVEN_REPO=https://repo.maven.apache.org/maven2
USER root
ADD --chown=spark:spark ${MAVEN_REPO}/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/
ADD --chown=spark:spark ${MAVEN_REPO}/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/
ADD --chown=spark:spark ${MAVEN_REPO}/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar /opt/spark/jars/

COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf
USER spark

ENV PATH=/opt/spark/bin:$PATH
