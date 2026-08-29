from pyspark import pipelines as dp

expectations = {
    "exp": "user_id IS NOT NULL"
}

@dp.table

def dimuser_stg():
    df = spark.readStream.table("spotify_cata.silver.dimuser")
    return df 

dp.create_streaming_table(
    name = "dimuser",
    expect_all_or_drop = expectations
    )

dp.create_auto_cdc_flow(
    target = "dimuser",
    source = "dimuser_stg",
    keys = ["user_id"],
    sequence_by= "updated_at",
    stored_as_scd_type= 2
) 