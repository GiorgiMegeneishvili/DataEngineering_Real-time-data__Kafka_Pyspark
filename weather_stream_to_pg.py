from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType

# Spark Session (PostgreSQL driver-ით)
spark = (SparkSession.builder
         .appName("Weather Kafka → PostgreSQL Streaming")
         .config("spark.jars.packages",
                 "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,"
                 "org.postgresql:postgresql:42.7.4")
         .config("spark.sql.streaming.checkpointLocation", "/tmp/weather_checkpoint")  # ← აუცილებელია!
         .getOrCreate())

spark.sparkContext.setLogLevel("ERROR")

# Read from Kafka
kafka_df = (spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "weather-stream")
            .option("startingOffsets", "latest")   # ან "earliest" თუ ძველი მონაცემებიც გინდა
            .load())

# Kafka value is bytes → string → JSON parse
schema = StructType([
    StructField("city", StringType(), False),
    StructField("temperature", FloatType(), False),
    StructField("humidity", IntegerType(), False),
    StructField("weather", StringType(), True),
    StructField("event_time", StringType(), False),   # ჯერ string
    StructField("pressure", IntegerType(), True),
    StructField("wind_speed", FloatType(), True),
    StructField("source", StringType(), True)
])

parsed_df = (kafka_df
             .selectExpr("CAST(value AS STRING)")
             .select(from_json(col("value"), schema).alias("data"))
             .select("data.*"))

# event_time string → Timestamp
from pyspark.sql.functions import to_timestamp
parsed_df = parsed_df.withColumn("event_time", to_timestamp(col("event_time")))

# Write to PostgreSQL (using foreachBatch — best for streaming)
def write_to_postgres(batch_df, batch_id):
    print(f"Batch {batch_id} — rows: {batch_df.count()}")
    if not batch_df.isEmpty():
        (batch_df.write
         .format("jdbc")
         .option("url", "jdbc:postgresql://localhost:5432/demodb")
         .option("dbtable", "weather_data1")
         .option("user", "postgres")
         .option("password", "Gio81268761")
         .option("driver", "org.postgresql.Driver")
         .mode("append")
         .save())

query = (parsed_df.writeStream
        .foreachBatch(write_to_postgres)
        .outputMode("append")
        .trigger(processingTime="30 seconds")   # რამდენ ხანში გადაამუშავოს batch
        .start())

print("Streaming query started... waiting for data from Kafka")

query.awaitTermination()   # runs until you stop it (Ctrl+C)
