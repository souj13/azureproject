from pyspark import pipelines as dp

@dp.table
def dimtrack_stg():
    df = spark.readStream.table("spotify_cata.silver.dimtrack")
    return df 

dp.create_streaming_table("dimtrack")

dp.create_auto_cdc_flow(
    target = "dimtrack",
    source = "dimtrack_stg",
    keys = ["track_id"],
    sequence_by= "updated_at",
    stored_as_scd_type= 2
) 