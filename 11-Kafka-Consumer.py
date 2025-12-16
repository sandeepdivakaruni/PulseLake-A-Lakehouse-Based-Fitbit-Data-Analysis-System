# Databricks notebook source
# MAGIC %run ./01-config

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

class Kafka_Consumer:
    def __init__(self, topic, base_dir, file_prefix):
        """
        topic:      Kafka topic name, e.g., "user_info" or "gym_login"
        base_dir:   Landing zone base path for THIS topic, e.g., f"{Conf.kafkaTopic}/user_info_landing"
        file_prefix:Filename prefix, e.g., "2-user_info" (final files become 2-user_info_<n>.json)
        """
        self.Conf = Config()
        self.topic = topic
        self.base_dir = base_dir.rstrip("/")
        self.file_prefix = file_prefix

        # Paths (unique per topic!)
        self.data_dir  = f"{self.base_dir}/data"
        self.ckpt_dir  = f"{self.base_dir}/checkpoint"
        self.counter_path = f"{self.base_dir}/file_counter.txt"

        # Kafka
        self.BOOTSTRAP_SERVER = "pkc-921jm.us-east-2.aws.confluent.cloud:9092"
        self.JAAS_MODULE = "org.apache.kafka.common.security.plain.PlainLoginModule"
        self.CLUSTER_API_KEY = "WLBMXFTRTW73LQQN"
        self.CLUSTER_API_SECRET = "cfltMji9WOLt7PvLBf8CmXszMwi4xpQ8TtkeXWiLk7zubxkHdnwoW4qx5Wn0eS1A"

    def _spark(self, app="KafkaData"):
        return SparkSession.builder.appName(f"{app}:{self.topic}").getOrCreate()

    def stop_leftovers(self):
        for a in self._spark().streams.active:
            if a.name and self.topic in a.name:
                try: a.stop()
                except Exception: pass

    def ingestFromKafka(self, maxOffsetsPerTrigger=10000, startingOffsets="earliest"):
        spark = self._spark("BronzeStream")
        return (
            spark.readStream
                 .format("kafka")
                 .option("kafka.bootstrap.servers", self.BOOTSTRAP_SERVER)
                 .option("kafka.security.protocol", "SASL_SSL")
                 .option("kafka.sasl.mechanism", "PLAIN")
                 .option("kafka.sasl.jaas.config",
                         f'{self.JAAS_MODULE} required username="{self.CLUSTER_API_KEY}" password="{self.CLUSTER_API_SECRET}";')
                 .option("subscribe", self.topic)
                 .option("startingOffsets", startingOffsets)
                 .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
                 .option("failOnDataLoss", "false")
                 .load()
        )

    # ---------- custom filename machinery ----------
    def _get_and_increment_counter(self):
        try:
            content = dbutils.fs.head(self.counter_path)
            counter = int(content.strip())
        except Exception:
            counter = 0
        new_counter = counter + 1
        dbutils.fs.put(self.counter_path, str(new_counter), overwrite=True)
        return new_counter
    


    def _write_with_custom_name(self, df, epoch):
        print(f"[{self.topic}] foreachBatch triggered with {df.count()} rows")
        if df.rdd.isEmpty():
            return
        tmp = f"{self.base_dir}/_temp_batch"
        # make the envelope: one JSON per Kafka record
        env = (df.select(
                    F.col("key").cast("string").alias("key"),
                    F.col("value").cast("string").alias("value"),
                    F.col("topic"),
                    F.col("partition").cast("int").alias("partition"),
                    F.col("offset").cast("long").alias("offset"),
                    F.col("timestamp").cast("long").alias("timestamp")
              ))
        # write exactly one file, then rename
        (env.coalesce(1).write.mode("overwrite").json(tmp))
        number = self._get_and_increment_counter()
        final_name = f"{self.file_prefix}_{number}.json"
        final_path = f"{self.base_dir}/{final_name}"
        part_files = [f.path for f in dbutils.fs.ls(tmp) if f.path.endswith(".json")]
        if part_files:
            dbutils.fs.mv(part_files[0], final_path, True)
        dbutils.fs.rm(tmp, recurse=True)
        print(f"[{self.topic}] Saved {final_name}")

    def process(self, maxOffsetsPerTrigger=10000,
                startingOffsets="earliest",  mode="availableNow", triggerInterval="60 seconds"):
        self.stop_leftovers()
        spark = self._spark()
        raw = self.ingestFromKafka(maxOffsetsPerTrigger, startingOffsets)

        writer = (raw.writeStream
                .foreachBatch(self._write_with_custom_name)
                .option("checkpointLocation", self.ckpt_dir)
                .trigger(processingTime=triggerInterval)
                .queryName(f"q_{self.topic}"))
        
        if mode == "continuous":
            q = writer.trigger(processingTime=triggerInterval).start()
        else:
            q = writer.trigger(availableNow=True).start()

        print(f"[{self.topic}] streaming started (mode={mode}).")
        q.awaitTermination()
        print(f"[{self.topic}] streaming finished.")
        return q


# COMMAND ----------

Conf = Config() 
user = Kafka_Consumer(
    topic="user_info",
    base_dir=f"{Conf.kafkaTopic}/user_info",
    file_prefix="2-user_info"
)
user.process(maxOffsetsPerTrigger=500000, mode="availableNow",startingOffsets="earliest")


# COMMAND ----------

Conf = Config() 
workout = Kafka_Consumer(
    topic="workout",
    base_dir=f"{Conf.kafkaTopic}/workout",
    file_prefix="4-workout"
)
workout.process(maxOffsetsPerTrigger=500000, mode="availableNow",startingOffsets="earliest")

# COMMAND ----------

Conf = Config()
workout = Kafka_Consumer(
    topic="bpm",
    base_dir=f"{Conf.kafkaTopic}/bpm",
    file_prefix="3-bpm"
)

workout.process(
    maxOffsetsPerTrigger=1000000,
    mode="availableNow",
    startingOffsets="earliest"
)
