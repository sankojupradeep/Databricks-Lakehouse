import dlt
from pyspark.sql.functions import col, sum as spark_sum, max as spark_max

@dlt.table(
    name="customer_transactions_fact",
    comment="Fact table with customer transactions, aggregated and joined"
)
def customer_transactions_fact():
    # Read Gold tables
    t = dlt.read("dim1")  # replace with your actual table name
    cp = dlt.read("dim2")
    te = dlt.read("dim3")

    # Join tables on customer_id and transaction_id
    joined_df = (
        t.alias("t")
        .join(cp.alias("cp"), on="customer_id", how="left")
        .join(te.alias("te"), on=col("t.transaction_id") == col("te.transaction_id"), how="left")
    )

    # Aggregate example: total amount per customer & transaction
    agg_df = joined_df.groupBy(
        "t.customer_id",
        "t.transaction_id",
        "cp.category",
        "cp.payment"
    ).agg(
        spark_sum("t.amount").alias("amount"),
        spark_max("t.timestamp").alias("timestamp")  # latest transaction time
    )

    # Select only required columns
    fact_df = agg_df.select(
        col("customer_id"),
        col("transaction_id"),
        col("amount"),
        col("category"),
        col("timestamp"),
        col("payment")
    )
    
    fact_df.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable("financial_transaction.default.fully_transformed_data")

    return fact_df
