from pyspark.sql.types import *
from pyspark.sql.functions import *
from src.Streaming_Pipeline.transformations.gold.gold_trade_summary import build_trade_summary



def test_dim_time():

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

    df = spark.createDataFrame(data, schema)

    result = create_gold_dim_time(df)

    row = result.collect()[0]

    assert row.date == 2026-07-03
    assert row.year == 2026
    assert row.month == 7
   