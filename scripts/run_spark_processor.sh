#!/bin/sh

SPARK_VERSION="${SPARK_VERSION:-3.5.6}"
KAFKA_PACKAGE="org.apache.spark:spark-sql-kafka-0-10_2.12:${SPARK_VERSION}"

uv run --env-file .env spark-submit --packages "${KAFKA_PACKAGE}" --conf "spark.sql.shuffle.partitions=3" spark/ticket_stream_processor.py
