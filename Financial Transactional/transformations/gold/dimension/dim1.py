import dlt
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number

@dlt.table(
    name="dim1",
    comment="Dimension table built from Silver layer"
)
def dimension1():
    df = dlt.read("transactions_silver")  # Silver table (can be streaming)
    
    # Deduplicate using window function (works in batch)
    w = Window.partitionBy("transaction_id").orderBy(col("timestamp").desc())
    dedup_df = df.withColumn("rn", row_number().over(w)) \
                 .filter(col("rn") == 1) \
                 .drop("rn")
    
    # Select required columns
    return dedup_df.select(
        "transaction_id",
        "customer_id",
        "amount",
        "currency",
        "location",
        "timestamp"
    )
