from pyspark.sql.functions import col
from pyspark.sql.types import *

from src.gold.market_summary import build_market_summary


def test_build_market_summary():

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
        ("BTC-USD", "2026-07-03 10:00:00", 60000.0),
        ("BTC-USD", "2026-07-03 10:05:00", 60500.0),
        ("ETH-USD", "2026-07-03 10:00:00", 2000.0),
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