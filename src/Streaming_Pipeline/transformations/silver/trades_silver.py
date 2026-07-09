from pyspark import pipelines as dp
from pyspark.sql.functions import col, from_json, schema_of_json, get_json_object 
from pyspark.sql.functions import * 
from pyspark.sql.types import *


tschema = StructType([StructField('event_type', StringType(), True), StructField('ingested_at', StringType(), True), StructField('payload', StructType([StructField('best_ask', StringType(), True), StructField('best_bid', StringType(), True), StructField('trades', ArrayType(StructType([StructField('ask', StringType(), True), StructField('bid', StringType(), True), StructField('exchange', StringType(), True), StructField('price', StringType(), True), StructField('product_id', StringType(), True), StructField('side', StringType(), True), StructField('size', StringType(), True), StructField('time', StringType(), True), StructField('trade_id', StringType(), True)]), True), True)]), True), StructField('product_id', StringType(), True)])



def create_trades_silver(df):
    # Get Ticker Schema ---
    ticker = ( df.filter(col("event_type") == "ticker") 
    )
 

    ticker_df = ( ticker.withColumn("data", from_json(col("trades"), tschema)) \
                        .select("data.event_type", "data.ingested_at", "data.payload.*")
    )


    ticker_df = ( ticker_df.withColumn("single_trade", explode(col('trades')))\
                            .select("event_type", "ingested_at", "best_ask", "best_bid", "single_trade.*") 
    )

    return ( ticker_df.withColumn("trade_timestamp", col('time').cast('timestamp'))\
                     .withColumn("ingested_at", col('ingested_at').cast('timestamp'))\
                     .withColumn("price", col("price").cast(DecimalType(18, 2)))\
                     .withColumn("size", col("size").cast(DecimalType(12, 10)))\
                     .withColumn("best_ask", col("best_ask").cast('double'))\
                     .withColumn("best_bid", col("best_bid").cast('double')) 

                    
    )




@dp.table 
@dp.expect_or_fail("unique_trade", "trade_id IS NOT NULL")
@dp.expect_or_drop("product_not_null", "product_id IS NOT NULL")
@dp.expect_or_drop("price_not_null", "price IS NOT NULL") 
@dp.expect_or_drop("size_value", "size > 0")
@dp.expect("side_value", "side in ('SELL', 'BUY')") 
def trades_silver():
    df = spark.readStream.table("stocks_raw")

    # Add an event_type column to route event data 
    df = df.withColumn("event_type", get_json_object(col("trades"), "$.event_type"))

    return create_trades_silver(df)


