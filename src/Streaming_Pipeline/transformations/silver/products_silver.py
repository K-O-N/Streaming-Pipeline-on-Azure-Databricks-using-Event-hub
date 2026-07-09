from pyspark import pipelines as dp
from pyspark.sql.functions import col, from_json, schema_of_json, get_json_object 
from pyspark.sql.functions import * 
from pyspark.sql.types import *



pschema = StructType([StructField('event_type', StringType(), True), StructField('ingested_at', StringType(), True), StructField('payload', StructType([StructField('about_description', StringType(), True), StructField('alias', StringType(), True), StructField('alias_to', ArrayType(StringType(), True), True), StructField('approximate_quote_24h_volume', StringType(), True), StructField('auction_mode', BooleanType(), True), StructField('base_cbrn', StringType(), True), StructField('base_currency_id', StringType(), True), StructField('base_display_symbol', StringType(), True), StructField('base_increment', StringType(), True), StructField('base_max_size', StringType(), True), StructField('base_min_size', StringType(), True), StructField('base_name', StringType(), True), StructField('best_ask_price', StringType(), True), StructField('best_bid_price', StringType(), True), StructField('cancel_only', BooleanType(), True), StructField('display_name', StringType(), True), StructField('display_name_overwrite', StringType(), True), StructField('fcm_trading_session_details', StringType(), True), StructField('high_24h', StringType(), True), StructField('icon_color', StringType(), True), StructField('icon_url', StringType(), True), StructField('is_alpha_testing', BooleanType(), True), StructField('is_disabled', BooleanType(), True), StructField('limit_only', BooleanType(), True), StructField('low_24h', StringType(), True), StructField('market_cap', StringType(), True), StructField('mid_market_price', StringType(), True), StructField('new', BooleanType(), True), StructField('new_at', StringType(), True), StructField('post_only', BooleanType(), True), StructField('price', StringType(), True), StructField('price_increment', StringType(), True), StructField('price_percentage_change_24h', StringType(), True), StructField('product_cbrn', StringType(), True), StructField('product_id', StringType(), True), StructField('product_type', StringType(), True), StructField('product_venue', StringType(), True), StructField('quote_cbrn', StringType(), True), StructField('quote_currency_id', StringType(), True), StructField('quote_display_symbol', StringType(), True), StructField('quote_increment', StringType(), True), StructField('quote_max_size', StringType(), True), StructField('quote_min_size', StringType(), True), StructField('quote_name', StringType(), True), StructField('status', StringType(), True), StructField('trading_disabled', BooleanType(), True), StructField('view_only', BooleanType(), True), StructField('volume_24h', StringType(), True), StructField('volume_percentage_change_24h', StringType(), True), StructField('watched', BooleanType(), True)]), True), StructField('product_id', StringType(), True)])



def create_products_silver(df):
    # Get product Schema 
    product = ( df.filter(col("event_type") == "product")
              )


    product_df = ( product.withColumn("data", from_json(col('trades'), pschema))\
                          .select("data.event_type", "data.ingested_at", "data.payload.*") 
                )

    product_df =  ( product_df.drop('alias', 'alias_to', 'about_description', 'auction_mode', 'base_cbrn', 'base_display_symbol', 'best_ask_price', 'best_bid_price', 'cancel_only', 'display_name', 'fcm_trading_session_details', 'high_24h', 'display_name_overwrite', 'icon_color', 'icon_url', 'product_cbrn', 'quote_cbrn', 'trading_disabled', 'view_only', 'is_alpha_testing', 'is_disabled', 'limit_only', 'low_24h', 'market_cap', 'mid_market_price', 'product')
                   
    )

    return ( product_df.withColumn('ingested_at', col('ingested_at').cast('timestamp'))\
                        .withColumn('new_at', col('new_at').cast('timestamp'))\
                        .withColumn('approximate_quote_24h_volume', col('approximate_quote_24h_volume').cast(DecimalType(18, 2)))\
                        .withColumn('base_increment', col('base_increment').cast('double'))\
                        .withColumn('base_max_size', col('base_max_size').cast('long'))\
                        .withColumn('base_min_size', col('base_min_size').cast('double'))\
                        .withColumn('price', col('price').cast('double'))\
                        .withColumn('price_increment', col('price_increment').cast('double'))\
                        .withColumn('price_percentage_change_24h', col('price_percentage_change_24h').cast(DecimalType(18, 8)))\
                        .withColumn('volume_24h', col('volume_24h').cast(DecimalType(18, 10)))\
                        .withColumn('volume_percentage_change_24h', col('volume_percentage_change_24h').cast('double')) 

    )

    



@dp.table
@dp.expect("product_id not null", "product_id is not null")
@dp.expect_or_fail("price_greater_than_zero", 'price > 0')
def products_silver():

    df = spark.readStream.table("dev_stock.bronze.stocks_raw")
    df = df.withColumn("event_type", get_json_object(col("trades"), "$.event_type"))

    return create_products_silver(df) 


                            
                            
                        

