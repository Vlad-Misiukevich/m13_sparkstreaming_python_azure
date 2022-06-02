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
    list_of_paths = [
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2016/month=10/day=01/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2016/month=10/day=02/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2016/month=10/day=03/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2016/month=10/day=04/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2016/month=10/day=05/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2016/month=10/day=06/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2016/month=10/day=07/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2016/month=10/day=08/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2016/month=10/day=09/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2016/month=10/day=10/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2016/month=10/day=11/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2016/month=10/day=12/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2016/month=10/day=13/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2016/month=10/day=14/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2016/month=10/day=15/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2016/month=10/day=16/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2016/month=10/day=17/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2016/month=10/day=18/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2016/month=10/day=19/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2016/month=10/day=20/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2016/month=10/day=21/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2016/month=10/day=22/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2016/month=10/day=23/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2016/month=10/day=24/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2016/month=10/day=25/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2016/month=10/day=26/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2016/month=10/day=27/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2016/month=10/day=28/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2016/month=10/day=29/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2016/month=10/day=30/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2016/month=10/day=31/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=08/day=01/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=08/day=02/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=08/day=03/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=08/day=04/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=08/day=05/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=08/day=06/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=08/day=07/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=08/day=08/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=08/day=09/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=08/day=10/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=08/day=11/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=08/day=12/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=08/day=13/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=08/day=14/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=08/day=15/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=08/day=16/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=08/day=17/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=08/day=18/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=08/day=19/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=08/day=20/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=08/day=21/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=08/day=22/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=08/day=23/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=08/day=24/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=08/day=25/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=08/day=26/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=08/day=27/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=08/day=28/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=08/day=29/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=08/day=30/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=08/day=31/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=09/day=01/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=09/day=02/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=09/day=03/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=09/day=04/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=09/day=05/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=09/day=06/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=09/day=07/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=09/day=08/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=09/day=09/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=09/day=10/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=09/day=11/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=09/day=12/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=09/day=13/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=09/day=14/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=09/day=15/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=09/day=16/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=09/day=17/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=09/day=18/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=09/day=19/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=09/day=20/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=09/day=21/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=09/day=22/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=09/day=23/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=09/day=24/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=09/day=25/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=09/day=26/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=09/day=27/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=09/day=28/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=09/day=29/',
        'abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/year=2017/month=09/day=30/'
    ]
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
