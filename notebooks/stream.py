# Databricks notebook source
spark.conf.set("fs.azure.account.key.stvmisiukevich.dfs.core.windows.net", "j8dXelw8GOllHRTTUoVwWr6l+ZJCGC1ONWn+oDKAqYQQVGdeVzW2WNzH9YPREJvErwlLNIEOxyQt+AStiGl8fA==") 
schema = ('address string, avg_tmpr_c double, avg_tmpr_f double, city string, country string, geoHash string, id string, latitude double, longitude double, \
          name string, wthr_date string, wthr_year string, wthr_month string, wthr_day string, year string, month string, day string')
upload_path = "abfss://data@stvmisiukevich.dfs.core.windows.net/weath-hot"

df = (spark
      .readStream
      .format('cloudFiles')
      .option('cloudFiles.format', 'parquet')
      .schema(schema)
      .load(upload_path))

# COMMAND ----------

from pyspark.sql.functions import approx_count_distinct, col


distinct_hotels = (df
                  .groupBy("wthr_date", "city")
                  .agg(
                       approx_count_distinct("id").alias("hotels_count")
                      ) 
                  .orderBy(col("hotels_count").desc()) 
                  )
display(distinct_hotels)


# COMMAND ----------

from pyspark.sql.functions import avg, min, max

temp_in_city = (
                df
                .groupBy("wthr_date", "city")
                .agg(
                     avg("avg_tmpr_c").alias("average_temperature"),
                     max("avg_tmpr_c").alias("max_temperature"),
                     min("avg_tmpr_c").alias("min_temperature")
                    )
                )
display(temp_in_city)

# COMMAND ----------

from pyspark.sql.functions import approx_count_distinct, col, avg, min, max


data_visualisation = (df
                  .groupBy("city")
                  .agg(
                       approx_count_distinct("id").alias("hotels_count"),
                       avg("avg_tmpr_c").alias("average_temperature"),
                       max("avg_tmpr_c").alias("max_temperature"),
                       min("avg_tmpr_c").alias("min_temperature"),
                      ) 
                  .orderBy(col("hotels_count").desc()) 
                  .limit(10) 
                  )
display(data_visualisation)
