import dlt
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number

@dlt.table(
    name="dim2",
    comment="Gold layer table with customer_id, status, payment, category, timestamp"
)
def customer_payment_gold():
    # Read from Silver layer
    df = dlt.read("transactions_silver")  # or bronze if you prefer

    # Deduplicate by customer_id keeping latest timestamp
    w = Window.partitionBy("customer_id").orderBy(col("timestamp").desc())
    dedup_df = df.withColumn("rn", row_number().over(w)) \
                 .filter(col("rn") == 1) \
                 .drop("rn")

    # Select required columns and rename if needed
    gold_df = dedup_df.select(
        "customer_id",
        col("status"),
        col("payment_method").alias("payment"),
        "category",
        "timestamp"
    )

    return gold_df
