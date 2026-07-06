from pyspark import pipelines as dp
from pyspark.sql.functions import * 
from pyspark.sql.types import *


# Event Hubs configuration
EH_NAMESPACE = "bstockevents"
EH_NAME  = "tradetopic"

EH_CONN_STR = spark.conf.get("connection_string")


# Kafka Consumer configuration
KAFKA_OPTIONS = {
  "kafka.bootstrap.servers"  : f"{EH_NAMESPACE}.servicebus.windows.net:9093",
  "subscribe"                : EH_NAME,
  "kafka.sasl.mechanism"     : "PLAIN",
  "kafka.security.protocol"  : "SASL_SSL",
  "kafka.sasl.jaas.config"   : f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{EH_CONN_STR}\";",
  "kafka.request.timeout.ms" : "60000",
  "kafka.session.timeout.ms" : "45000",
  "kafka.heartbeat.interval.ms": "15000", 
  "maxOffsetsPerTrigger"     : "4500",
  "failOnDataLoss"           : 'true',
  "startingOffsets"          : 'earliest'
}


@dp.table
def stocks_raw():
  df = spark.readStream.format("kafka")\
            .options(**KAFKA_OPTIONS)\
            .load()

  # Convert binary datatypes to string
  df = df.withColumn("trades", col("value").cast("string"))
 

  return df 

