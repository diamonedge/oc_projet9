FROM python:3.11-slim-bookworm

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
 curl openjdk-17-jre-headless && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"
ENV PYSPARK_PYTHON=/app/.venv/bin/python
ENV PYSPARK_DRIVER_PYTHON=/app/.venv/bin/python

RUN pip install --no-cache-dir uv

COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --frozen --no-dev

COPY spark/ ./spark/

CMD ["uv", "run", "spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6", "--conf", "spark.sql.shuffle.partitions=3", "spark/ticket_stream_processor.py"]
