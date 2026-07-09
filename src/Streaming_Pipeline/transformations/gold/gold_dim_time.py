from pyspark import pipelines as dp
from pyspark.sql.window import Window
from pyspark.sql.functions import * 
from pyspark.sql.types import * 




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

    


@dp.materialized_view
def gold_dim_time():
    df = spark.read.table("trades_silver")


    return create_gold_dim_time(df)


