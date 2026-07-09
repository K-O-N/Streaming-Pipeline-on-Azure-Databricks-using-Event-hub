from pyspark.sql.types import *
from pyspark.sql.functions import * 
from src.gold.trade_summary import build_trade_summary


def test_build_trade_summary(spark):

    schema = StructType([
            StructField("product_id", StringType()),
            StructField("trade_timestamp", TimestampType()),
            StructField("side", StringType()),
            StructField("price", DoubleType()),
            StructField("size", DoubleType())
        ])

    data = [
        ("BTC-USD", "2026-07-03 10:00:00", "BUY", 100.0, 2.0),
        ("BTC-USD", "2026-07-03 10:01:00", "SELL", 110.0, 1.0),
        ("BTC-USD", "2026-07-03 10:02:00", "BUY", 120.0, 3.0),
    ]

    df = spark.createDataFrame(data, schema=schema)

    result = build_trade_summary(df)

    row = result.collect()[0]

    assert row.buy_trades_count == 2
    assert row.sell_trades_count == 1

    assert row.total_volume == 6.0
    assert row.buy_volume == 5.0
    assert row.sell_volume == 1.0

    assert row.buy_trade_value == 560.0
    assert row.sell_trade_value == 110.0

    assert round(row.buy_percentage, 4) == 0.8333
    assert round(row.sell_percentage, 4) == 0.1667