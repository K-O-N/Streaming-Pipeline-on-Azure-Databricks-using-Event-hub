from pyspark.sql.functions import *
from pyspark.sql.types import *
import pytest

from datetime import datetime, date
from src.Streaming_Pipeline.transformations.gold.gold_functions import create_gold_dim_time



def test_dim_time(spark):

    schema = StructType([
        StructField("trade_id", StringType()),
        StructField("product_id", StringType()),
        StructField("trade_timestamp", TimestampType()),
        StructField("side", StringType()),
        StructField("price", DoubleType()),
        StructField("size", DoubleType())
    ])

    data = [
        ("1049502707", "BTC-USD", datetime(2026, 7, 3, 10, 0, 0), "BUY", 100.0, 2.0),
        ("1049502706",  "BTC-USD", datetime(2026, 7, 3, 10, 1, 0), "SELL", 110.0, 1.0),
        ("1049502705", "BTC-USD", datetime(2026, 7, 3, 10, 3, 0), "BUY", 120.0, 3.0),
    ]

    df = spark.createDataFrame(data, schema)

    result = create_gold_dim_time(df)

    row = result.collect()[0]

    assert row.date == date(2026, 7, 3)
    assert row.year == 2026
    assert row.month == 7
