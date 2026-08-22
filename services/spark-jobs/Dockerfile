FROM python:3.12-slim
WORKDIR /opt/spark-jobs

RUN apt-get update && apt-get install -y --no-install-recommends openjdk-21-jre-headless procps && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    setuptools \
    pyspark==3.5.3 \
    "redis>=5.0" \
    psycopg2-binary \
    "numpy>=1.26" \
    "kafka-python-ng>=2.2" \
    "mlflow>=2.13"

# Pre-download Kafka connector JAR at build time so streaming.py starts offline
RUN python -c "\
from pyspark.sql import SparkSession; \
SparkSession.builder \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3') \
    .config('spark.ui.enabled', 'false') \
    .getOrCreate().stop()"

COPY bootstrap.py streaming.py ./
