import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, when
from pyspark.sql.types import StringType, StructField, StructType


BOOTSTRAP_SERVERS = os.getenv("REDPANDA_BOOTSTRAP_SERVERS", "localhost:19092")
TOPIC = os.getenv("REDPANDA_TOPIC", "client_tickets")
OUTPUT_BASE_DIR = os.getenv("SPARK_OUTPUT_BASE_DIR", "data/output")
CHECKPOINT_DIR = os.getenv(
    "SPARK_CHECKPOINT_DIR",
    "data/checkpoint/ticket_stream_processor",
)


ticket_schema = StructType(
    [
        StructField("ticket_id", StringType(), False),
        StructField("client_id", StringType(), False),
        StructField("created_at", StringType(), False),
        StructField("request", StringType(), False),
        StructField("request_type", StringType(), False),
        StructField("priority", StringType(), False),
    ]
)


def enrich_tickets(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("created_at_ts", to_timestamp(col("created_at")))
        .withColumn(
            "support_team",
            when(col("request_type") == "technical", "Support technique")
            .when(col("request_type") == "billing", "Support facturation")
            .when(col("request_type") == "commercial", "Support commercial")
            .when(col("request_type") == "account", "Support compte client")
            .when(col("request_type") == "incident", "Cellule incident")
            .otherwise("Support général"),
        )
    )


def write_batch(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        print(f"[batch_id={batch_id}] Aucun ticket à traiter.")
        return

    enriched_df = enrich_tickets(batch_df).cache()

    enriched_output = f"{OUTPUT_BASE_DIR}/enriched_tickets"
    by_type_output = f"{OUTPUT_BASE_DIR}/latest/tickets_by_type"
    by_priority_output = f"{OUTPUT_BASE_DIR}/latest/tickets_by_priority"

    enriched_df.write.mode("append").json(enriched_output)

    (
        enriched_df.groupBy("request_type", "support_team")
        .count()
        .coalesce(1)
        .write.mode("overwrite")
        .json(by_type_output)
    )

    (
        enriched_df.groupBy("priority")
        .count()
        .coalesce(1)
        .write.mode("overwrite")
        .json(by_priority_output)
    )

    print(f"[batch_id={batch_id}] Tickets traités et exportés.")

    enriched_df.unpersist()


def main() -> None:
    spark = (
        SparkSession.builder.appName("client-ticket-stream-processor")
        .config("spark.sql.shuffle.partitions", "3")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    tickets_stream = (
        raw_stream.selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), ticket_schema).alias("ticket"))
        .select("ticket.*")
        .where(col("ticket_id").isNotNull())
    )

    query = (
        tickets_stream.writeStream.foreachBatch(write_batch)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime="10 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
