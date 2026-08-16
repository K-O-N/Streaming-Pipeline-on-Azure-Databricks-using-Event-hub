from pyspark.sql.functions import col
from pyspark.sql.types import *
import pytest 

from datetime import datetime
from src.Streaming_Pipeline.transformations.gold.gold_functions import build_market_summary


def test_build_market_summary(spark):

    product_schema = StructType([
        StructField("product_id", StringType()),
        StructField("ingested_at", TimestampType()),
        StructField("price", DoubleType())
    ])

    ticker_schema = StructType([
        StructField("product_id", StringType()),
        StructField("best_bid", DoubleType()),
        StructField("best_ask", DoubleType())
    ])

    product_data = [
        ("BTC-USD", datetime(2026, 7, 3, 10, 0, 0), 60000.0), 
        ("BTC-USD", datetime(2026, 7, 3, 10, 5, 0), 60500.0),
        ("ETH-USD", datetime(2026, 7, 3, 10, 6, 0), 2000.0),
    ]

    ticker_data = [
        ("BTC-USD", 60490.0, 60510.0),
        ("BTC-USD", 60500.0, 60520.0),
        ("ETH-USD", 1995.0, 2005.0),
    ]

    product_df = spark.createDataFrame(product_data, product_schema)

    ticker_df = spark.createDataFrame(ticker_data, ticker_schema)

    result = build_market_summary(product_df, ticker_df)

    btc = result.filter(col("product_id") == "BTC-USD").collect()[0]

    assert btc.price == 60500.0
    assert btc.max_bid == 60500.0
    assert btc.max_ask == 60520.0

    assert result.count() == 2 

