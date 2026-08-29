# Databricks notebook source
parameters = [
    {
        "table": "spotify_cata.gold.factstream",
        "alias": "factstream",
        "cols": "factstream.stream_id, factstream.listen_duration, factstream.user_id, factstream.track_id, factstream.date_key"
    },
    {
        "table": "spotify_cata.gold.dimuser",
        "alias": "dimuser",
        "cols": "dimuser.user_name",
        "condition": "factstream.user_id = dimuser.user_id"
    },
    {
        "table": "spotify_cata.gold.dimtrack",
        "alias": "dimtrack",
        "cols": "dimtrack.track_name",
        "condition": "factstream.track_id = dimtrack.track_id"
    },
    {
        "table": "spotify_cata.gold.dimdate",
        "alias": "dimdate",
        "cols": "dimdate.date, dimdate.day, dimdate.month, dimdate.year, dimdate.weekday",
        "condition": "factstream.date_key = dimdate.date_key"
    }
]

# COMMAND ----------

pip install jinja2

# COMMAND ----------

from jinja2 import Template

# COMMAND ----------

query_text = """

        SELECT
            {% for param in parameters %}
                {{param['cols']}}
                    {% if not loop.last %}
                        ,
                    {% endif%}
            {% endfor %}
        FROM
            {{parameters[0]['table']}} as {{parameters[0]['alias']}}

        {% for param in parameters[1:] %}
            LEFT JOIN    
                    {{param['table']}} as {{param['alias']}}
            ON
                {{param['condition']}}
        {% endfor %}
"""

# COMMAND ----------

jinja_sql = Template(query_text)
query = jinja_sql.render(parameters = parameters)
print(query)

# COMMAND ----------

joined_df = spark.sql(query)

# COMMAND ----------

joined_df.write\
    .format("delta")\
    .mode("overwrite")\
    .option("overwriteSchema", True)\
    .saveAsTable("spotify_cata.gold.factstream_enriched")

# COMMAND ----------

display(spark.sql("""
    DESCRIBE spotify_cata.gold.factstream
"""))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from spotify_cata.gold.factstream_enriched limit 20;

# COMMAND ----------

