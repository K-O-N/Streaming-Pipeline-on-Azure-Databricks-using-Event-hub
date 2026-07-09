from pyspark import pipelines as dp
from pyspark.sql.window import Window
from pyspark.sql.functions import * 
from pyspark.sql.types import * 



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



@dp.materialized_view
def gold_market_summary():
    product_df = spark.read.table("products_silver")
    ticker_df = spark.read.table("trades_silver") 

    return build_market_summary(product_df, ticker_df)



