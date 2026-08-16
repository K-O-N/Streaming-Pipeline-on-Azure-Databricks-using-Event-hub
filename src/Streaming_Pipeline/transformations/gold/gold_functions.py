from pyspark.sql.window import Window
from pyspark.sql.functions import * 
from pyspark.sql.types import * 


# gold dim time 
def create_gold_dim_time(df):

    return (df.withColumn('date', to_date(col('trade_timestamp')))\
            .withColumn('year', year(col('trade_timestamp')))\
            .withColumn('month', month(col('trade_timestamp')))\
            .withColumn('day', dayofmonth(col('trade_timestamp')))\
            .withColumn('weekday', dayofweek(col('trade_timestamp')))\
            .withColumn('hour', hour(col('trade_timestamp')))\
            .withColumn('minute', minute(col('trade_timestamp')))\
            .select('trade_id', 'trade_timestamp', 'date', 'year', 'month', 'day', 'hour', 'minute')
    )


# gold fact market 
def create_fact_market_ohlc_hourly(df):

    windspec = Window.partitionBy('time_hour', 'product_id')
   
    df = ( df.withColumn('time_hour', date_trunc('hour', col('start')))\
                .withColumn('row_asc', row_number().over(windspec.orderBy('start')))\
                .withColumn('row_dsc', row_number().over(windspec.orderBy(desc('start'))))

          )
    

    return (
            df.groupBy('time_hour', 'product_id').agg(first(when(col('row_asc')==1, col('open')), ignorenulls=True).alias('open_price'),\
                                            max('high').alias('high_price'),
                                            min('low').alias('low_price'),
                                            first(when(col('row_dsc')==1, col('open')), ignorenulls=True).alias('close_price'),
                                            sum('volume').alias('hourly_volume'))
           ) 
    

# gold market summary
def build_market_summary(product_df, ticker_df):

    df = ( product_df.withColumn("latest_id", row_number()\
                        .over(Window.partitionBy("product_id").orderBy(desc("ingested_at"))))\
                        .filter(col("latest_id")==1)\
                        .withColumnRenamed('ingested_at', 'updated_at')\
                        .drop("latest_id") 

    )

    best_bid_ask = ( ticker_df.groupBy('product_id')\
                            .agg(max('best_bid').alias('max_bid'), max('best_ask').alias('max_ask'))

    )

    return(  df.join(best_bid_ask, "product_id")
           )
    

# gold trade summary
def build_trade_summary(df):
   
    df =   (  df.groupby('product_id', to_date('trade_timestamp').alias('trade_day'))\
                    .agg(count(when(col('side')=='BUY', True)).alias('buy_trades_count'),
                        count(when(col('side')=='SELL', True)).alias('sell_trades_count'),
                        sum(col('size')).alias('total_volume'),
                        sum(when(col('side')=='BUY', col('size'))).alias('buy_volume'),
                        sum(when(col('side')=='SELL', col('size'))).alias('sell_volume'),
                        sum(when(col('side')=='BUY', col('price') * col('size'))).alias('buy_trade_value'),
                        sum(when(col('side')=='SELL', col('price') * col('size'))).alias('sell_trade_value')
                        )
                    
    )
                    
    return ( df.withColumn('buy_percentage', col('buy_volume') / col('total_volume'))\
               .withColumn('sell_percentage', col('sell_volume') / col('total_volume')))
    


