from pyspark import pipelines as dp
from pyspark.sql.functions import col, from_json, schema_of_json, get_json_object 
from pyspark.sql.functions import * 
from pyspark.sql.types import * 


cschema = StructType([StructField('event_type', StringType(), True), StructField('ingested_at', StringType(), True), StructField('payload', StructType([StructField('close', StringType(), True), StructField('high', StringType(), True), StructField('low', StringType(), True), StructField('open', StringType(), True), StructField('start', StringType(), True), StructField('volume', StringType(), True)]), True), StructField('product_id', StringType(), True)])




def create_candles_silver(df):
    # Get candles schema 
    candle = ( df.filter(col("event_type") == "candle") 
              )

    candle_df = ( candle.withColumn("data", from_json(col('trades'), cschema))\
                        .select("data.event_type", "data.product_id", "data.ingested_at", "data.payload.*")

    ) 

    return ( candle_df.withColumn('ingested_at', col('ingested_at').cast('timestamp'))\
                        .withColumn("start", from_unixtime(col("start")))\
                        .withColumn('close', col('close').cast('double'))\
                        .withColumn('high', col('high').cast('double'))\
                        .withColumn('low', col('low').cast('double'))\
                        .withColumn('open', col('open').cast('double'))\
                        .withColumn('volume', col('volume').cast('double')) 

    )




@dp.table 
@dp.expect("open_valid", "open IS NOT NULL")
@dp.expect("close_valid", "close IS NOT NULL")
@dp.expect("high_valid", "high IS NOT NULL")
@dp.expect("low_valid", "low IS NOT NULL")
def candles_silver():
    df = spark.readStream.table("dev_stock.bronze.stocks_raw")

    # Add an event_type column
    df = df.withColumn("event_type", get_json_object(col("trades"), "$.event_type"))
    
    return create_candles_silver(df)
