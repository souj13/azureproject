# Databricks notebook source
# MAGIC %md
# MAGIC **DimUsers**

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

new_path = os.path.join(os.getcwd(),"..","..")
sys.path.append(new_path)

from utils.transformations import reusable

# COMMAND ----------

df = spark.read.format("parquet")\
        .load("abfss://bronze@tokyoolympicssj.dfs.core.windows.net/dimuser")

# COMMAND ----------

# MAGIC %md
# MAGIC #Autoloader#

# COMMAND ----------

df_user = spark.readStream\
            .format("cloudFiles")\
            .option("cloudFiles.format","parquet")\
            .option("cloudFiles.schemaLocation","abfss://silver@tokyoolympicssj.dfs.core.windows.net/dimuser/schema")\
            .load("abfss://bronze@tokyoolympicssj.dfs.core.windows.net/dimuser/")

# COMMAND ----------

df_user = df_user.withColumn(
    "user_name",
    upper(col("user_name"))
)

# COMMAND ----------

df_user_obj = reusable()
df_user = df_user_obj.dropColumns(df_user,['_rescued_data'])

# COMMAND ----------

df_user.writeStream\
        .format("delta")\
        .outputMode("append")\
        .option("checkpointLocation","abfss://silver@tokyoolympicssj.dfs.core.windows.net/dimuser/checkpoint")\
        .trigger(availableNow = True)\
        .start("abfss://silver@tokyoolympicssj.dfs.core.windows.net/dimuser/data")

# COMMAND ----------

df_art = spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format","parquet")\
            .option("cloudFiles.schemaLocation","abfss://silver@tokyoolympicssj.dfs.core.windows.net/dimartist/schema")\
            .load("abfss://bronze@tokyoolympicssj.dfs.core.windows.net/dimartist/")

# COMMAND ----------

df_art_obj = reusable()
df_art = df_art_obj.dropColumns(df_art,["_rescued_data"])

# COMMAND ----------

df_art.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation","abfss://silver@tokyoolympicssj.dfs.core.windows.net/dimartist/checkpoint")\
    .trigger(availableNow = True)\
    .toTable("spotify_cata.silver.DimArtist")

# COMMAND ----------

df_track = spark.readStream.format("cloudFiles")\
                .option("cloudFiles.format","parquet")\
                .option("cloudFiles.schemaLocation","abfss://silver@tokyoolympicssj.dfs.core.windows.net/dimtrack/schema")\
                .load("abfss://bronze@tokyoolympicssj.dfs.core.windows.net/dimtrack/")

# COMMAND ----------

df_track = (
            df_track.withColumn(
                "durationFlag",
                when(
                    col("duration_sec") < 150,
                    "low"
                )
                .when(
                    col("duration_sec") < 300,
                    "medium"
                ).otherwise("high")
            )
            .withColumn(
                "track_name",
                regexp_replace(
                    col("track_name"),
                    "[~@$^&-]",
                    " "
                )
            )
)

# COMMAND ----------

# DBTITLE 1,s
df_track_obj = reusable()
df_track = df_track_obj.dropColumns(df_track,["_rescued_data"])

# COMMAND ----------

df_track.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation","abfss://silver@tokyoolympicssj.dfs.core.windows.net/dimtrack/checkpoint")\
    .option("path", "abfss://silver@tokyoolympicssj.dfs.core.windows.net/dimtrack/data")\
    .trigger(availableNow = True)\
    .toTable("spotify_cata.silver.DimTrack")

# COMMAND ----------

df_user.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation","abfss://silver@tokyoolympicssj.dfs.core.windows.net/dimuser/checkpoint")\
    .option("path", "abfss://silver@tokyoolympicssj.dfs.core.windows.net/dimuser/data")\
    .trigger(availableNow = True)\
    .toTable("spotify_cata.silver.DimUser")

# COMMAND ----------

df_date = spark.readStream.format("cloudFiles")\
                .option("cloudFiles.format","parquet")\
                .option("cloudFiles.schemaLocation","abfss://silver@tokyoolympicssj.dfs.core.windows.net/dimdate/schema")\
                .load("abfss://bronze@tokyoolympicssj.dfs.core.windows.net/dimdate/")

# COMMAND ----------

df_date_obj = reusable()
df_date = df_date_obj.dropColumns(df_date,["_rescued_data"])

# COMMAND ----------

df_date.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation","abfss://silver@tokyoolympicssj.dfs.core.windows.net/dimdate/checkpoint")\
    .option("path", "abfss://silver@tokyoolympicssj.dfs.core.windows.net/dimdate/data")\
    .trigger(availableNow = True)\
    .toTable("spotify_cata.silver.DimDate")

# COMMAND ----------

df_fact = spark.readStream.format("cloudFiles")\
                .option("cloudFiles.format","parquet")\
                .option("cloudFiles.schemaLocation","abfss://silver@tokyoolympicssj.dfs.core.windows.net/factstream/schema")\
                .load("abfss://bronze@tokyoolympicssj.dfs.core.windows.net/factstream/")

# COMMAND ----------

df_fact_obj = reusable()
df_fact = df_fact_obj.dropColumns(df_fact,["_rescued_data"])

# COMMAND ----------

df_fact.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation","abfss://silver@tokyoolympicssj.dfs.core.windows.net/factstream/checkpoint")\
    .option("path", "abfss://silver@tokyoolympicssj.dfs.core.windows.net/factstream/data")\
    .trigger(availableNow = True)\
    .toTable("spotify_cata.silver.factstream")

# COMMAND ----------

