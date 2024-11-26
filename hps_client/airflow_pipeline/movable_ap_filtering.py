import findspark
import pandas as pd

from pyspark.sql import SparkSession
from sedona.register import SedonaRegistrator
from sedona.utils import SedonaKryoRegistrator, KryoSerializer
import shapely.speedups
from datetime import date, timedelta
import datetime
import pyspark.sql.functions as F
from pyspark.sql.types import FloatType
from pyspark.sql.functions import explode
from pyspark.sql.functions import col
import time
from pyspark.sql.functions import lower, upper
from pyspark.sql.functions import expr, collect_list, arrays_zip
from shapely.geometry import Point, MultiPoint

from pyspark.sql.types import *
import pyspark.sql.functions as f
from sedona.spark import *

findspark.init()
spark = SparkSession.builder. \
        master("spark://172.27.11.56:7077").\
        appName("filteringData").\
        config("spark.driver.memory","60g").\
        config("spark.executor.memory","20g").\
        config("spark.driver.maxResultSize","3g").\
        config("spark.driver.cores","3").\
        config("spark.dynamicAllocation.enabled","true").\
        config("spark.sql.adaptive.enabled","true").\
        config("spark.sql.adaptive.coalescePartitions.enabled","true").\
        config("spark.executor.instances", 10).\
        config("spark.executor.cores", 4).\
        config("spark.hadoop.fs.s3a.endpoint", "http://172.27.11.56:9000").\
        config("spark.hadoop.fs.s3a.access.key", "hps_client_key").\
        config("spark.hadoop.fs.s3a.secret.key", "qwer4321!").\
        config("spark.hadoop.fs.s3a.connection.maximum", 400).\
        config("spark.hadoop.fs.s3a.threads.max", 200).\
        config("spark.hadoop.fs.s3a.fast.upload", "true").\
        config("spark.sql.execution.pyspark.enabled", "true").\
        config("spark.sql.execution.arrow.enabled", "true").\
        config("spark.local.dir", "/home/svcapp_su/spark").\
        getOrCreate()


config = SedonaContext.builder(). \
          master("spark://172.27.11.56:7077").\
          config('spark.jars.packages',
           'org.apache.sedona:sedona-spark-3.4_2.12:1.5.1,'
           'org.datasyslab:geotools-wrapper:1.5.1-28.2'). \
           config('spark.jars.repositories', 'https://artifacts.unidata.ucar.edu/repository/unidata-all'). \
           appName("filteringData2").\
           config("spark.driver.memory","60g").\
           config("spark.executor.memory","20g").\
           config("spark.driver.maxResultSize","10g").\
           config("spark.dynamicAllocation.enabled","true").\
           config("spark.sql.adaptive.enabled","true").\
           config("spark.sql.adaptive.coalescePartitions.enabled","true").\
           config("spark.executor.instances", 10).\
           config("spark.executor.cores", 4).\
           config("spark.hadoop.fs.s3a.endpoint", "http://172.27.11.56:9000").\
           config("spark.hadoop.fs.s3a.access.key", "hps_client_key").\
           config("spark.hadoop.fs.s3a.secret.key", "qwer4321!").\
           config("spark.sql.execution.pyspark.enabled", "true").\
           config("spark.sql.execution.arrow.enabled", "true").\
           config("spark.ui.port", "4040") . \
           getOrCreate()

sedona = SedonaContext.create(config)
sc = sedona.sparkContext    


def main() :
   
   out =sedona.read.format("parquet").load("s3a://temp/data_set.parquet")
   out.createOrReplaceTempView("data")
   query = f"""
          select *
          from data 
          where not exists (
          select apMACAddress
          from (select apMACAddress, max(CAST(gt_latitude/(20*10) AS INT)*20*10 ) as max_gt_latitude , max(CAST(gt_longitude/(20*10) AS INT)*20*10) as max_gt_longitude ,min(CAST(gt_latitude/(20*10) AS INT)*20*10) as min_gt_latitude , min(CAST(gt_longitude/(20*10) AS INT)*20*10) as min_gt_longitude 
               from data
               group by apMACAddress
               ) temp  
          where  CAST((SQRT(POWER(max_gt_latitude-min_gt_latitude, 2) + POWER(max_gt_longitude-min_gt_longitude, 2)))/10 AS INT) > 1130   ) 
       """


   data_set = sedona.sql(query)

   data_set.write.mode("overwrite").parquet("s3a://temp/mv_data_set.parquet")

   # dnn data set 지역별로 만들기 


if __name__ == "__main__":
   
   try :
       #findspark.init()
       print("start")
       start = time.time()

       main()
       end = time.time()

       print(f"{end - start:.5f} sec")
   except Exception as e :
       print(e)
       spark.stop()
       #spark.close()
   finally :
       spark.stop()