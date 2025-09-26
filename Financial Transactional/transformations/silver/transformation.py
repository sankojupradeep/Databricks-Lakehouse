import dlt
from pyspark.sql.functions import col

dlt.create_streaming_table(
    name="transactions_silver",
    comment="Cleaned and standardized transactions from bronze"
)
@dlt.append_flow(target="transactions_silver")
def transactions_silver():
    bronze_df = spark.readStream.table("data_ingest")

    cleaned_df = (
        bronze_df
        # Fill missing values
        .na.fill({
            "amount": 0,
            "payment_method": "Unknown",
            "location": "NA"
        })
        # Drop rows where customer_id is null (must-have field)
        .na.drop(subset=["customer_id"])
    )

    return cleaned_df
