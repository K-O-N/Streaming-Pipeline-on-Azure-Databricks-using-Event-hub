from pyspark import pipelines as dp
from pyspark.sql.window import Window
from pyspark.sql.functions import * 
from pyspark.sql.types import * 




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
    
    
@dp.materialized_view
def gold_trade_summary():
    df = spark.read.table("trades_silver")

    return build_trade_summary(df)



    


