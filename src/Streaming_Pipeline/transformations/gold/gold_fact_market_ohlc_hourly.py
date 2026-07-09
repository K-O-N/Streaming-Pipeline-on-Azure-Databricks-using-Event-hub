from pyspark import pipelines as dp
from pyspark.sql.window import Window
from pyspark.sql.functions import * 
from pyspark.sql.types import * 



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


@dp.materialized_view
def gold_fact_market_ohlc_hourly():
    df = spark.read.table("candles_silver")


    return create_fact_market_ohlc_hourly(df) 


