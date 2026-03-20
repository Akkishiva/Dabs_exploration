from pyspark import pipelines as dp
from pyspark.sql.functions import col, sum

@dp.table
def high_revenue_zones():
    return (
        spark.read.table("sample_zones_bookstore")
        .filter(col("total_fare") > 10000)
    )