import os.path
import os.path
import sys
import time

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession


def get_spark_obj() -> SparkSession:
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    app_name = "DataExtract"
    master = "local[*]"
    spark_conf = SparkConf() \
        .setAppName(app_name) \
        .setMaster(master) \
        .set('spark.jars.packages', 'org.apache.hadoop:hadoop-azure:3.2.2') \
        .set("fs.azure.account.auth.type.bd201stacc.dfs.core.windows.net", "OAuth") \
        .set("fs.azure.account.oauth.provider.type.bd201stacc.dfs.core.windows.net",
             "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") \
        .set("fs.azure.account.oauth2.client.secret.bd201stacc.dfs.core.windows.net",
             "mAwIU~M4~xMYHi4YX_uT8qQ.ta2.LTYZxT") \
        .set("fs.azure.account.oauth2.client.endpoint.bd201stacc.dfs.core.windows.net",
             "https://login.microsoftonline.com/b41b72d0-4e9f-4c26-8a69-f949f367c91d/oauth2/token") \
        .set("fs.azure.account.oauth2.client.id.bd201stacc.dfs.core.windows.net",
             "f3905ff9-16d4-43ac-9011-842b661d556d") \
        .set("spark.executor.heartbeatInterval", "110s")
    return SparkSession.builder.config(conf=spark_conf).getOrCreate()


def copy_data_to_adls_with_delay(spark_obj: SparkSession) -> None:
    for path in list_of_paths:
        df = spark_obj.read.format("parquet").load(path)
        spark_obj.conf.set(
            "fs.azure.account.key.stvmisiukevich.dfs.core.windows.net",
            "j8dXelw8GOllHRTTUoVwWr6l+ZJCGC1ONWn+oDKAqYQQVGdeVzW2WNzH9YPREJvErwlLNIEOxyQt+AStiGl8fA=="
        )
        df.write.format("parquet").mode("append").save("abfss://data@stvmisiukevich.dfs.core.windows.net/weath-hot")
        time.sleep(30)


if __name__ == '__main__':
    spark = get_spark_obj()
    copy_data_to_adls_with_delay(spark)
