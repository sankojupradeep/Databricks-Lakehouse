import dlt

dlt.create_streaming_table(
    name="data_ingest"
)

@dlt.append_flow(target="data_ingest")
def transaction():
    df= spark.readStream.table("financial_transaction.default.transactions_bronze")
    return df