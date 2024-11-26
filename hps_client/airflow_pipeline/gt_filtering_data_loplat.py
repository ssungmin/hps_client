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
from shapely.geometry import Point, MultiPoint, Polygon
from pyspark.sql.functions import expr
import geopandas as gpd
from shapely import wkt
from pyspark.sql.types import *
import pyspark.sql.functions as f
from sedona.spark import *
from pyspark.sql.types import *

findspark.init()
spark = SparkSession.builder. \
        master("spark://172.27.11.56:7077").\
        appName("gt_filteringData").\
        config("spark.driver.memory","50g").\
        config("spark.executor.memory","10g").\
        config("spark.driver.cores","3").\
        config("spark.dynamicAllocation.enabled","true").\
        config("spakr.driver.maxResultSize","20g").\
        config("spark.sql.adaptive.enabled","true").\
        config("spark.sql.adaptive.coalescePartitions.enabled","true").\
        config("spark.executor.instances", 10).\
        config("spark.executor.cores", 3).\
        config("spark.hadoop.fs.s3a.endpoint", "http://172.27.11.56:9000").\
        config("spark.hadoop.fs.s3a.access.key", "hps_client_key").\
        config("spark.hadoop.fs.s3a.secret.key", "qwer4321!").\
        config("spark.sql.execution.pyspark.enabled", "true").\
        config("spark.sql.execution.arrow.enabled", "true").\
        config("spark.local.dir", "/home/svcapp_su/spark").\
        getOrCreate()


config = SedonaContext.builder(). \
         master("spark://172.27.11.56:7077"). \
         config('spark.jars.packages' ,
                'org.apache.sedona:sedona-spark-3.4_2.12:1.5.1, '
                'org.datasyslab:geotools-wrapper:1.5.1-28.2'). \
         config('sparl.jars.repositories', 'https://artifacts.unidata.ucar.edu/repository/unidata-all'). \
                 config("spark.driver.memory","50g").\
        config("spark.executor.memory","10g").\
        config("spark.driver.cores","3").\
        config("spark.dynamicAllocation.enabled","true").\
        config("spakr.driver.maxResultSize","20g").\
        config("spark.sql.adaptive.enabled","true").\
        config("spark.sql.adaptive.coalescePartitions.enabled","true").\
        config("spark.executor.instances", 10).\
        config("spark.executor.cores", 3).\
        config("spark.hadoop.fs.s3a.endpoint", "http://172.27.11.56:9000").\
        config("spark.hadoop.fs.s3a.access.key", "hps_client_key").\
        config("spark.hadoop.fs.s3a.secret.key", "qwer4321!").\
        config("spark.sql.execution.pyspark.enabled", "true").\
        config("spark.sql.execution.arrow.enabled", "true").\
        config("spark.local.dir", "/home/svcapp_su/spark").\
        getOrCreate()

sedona = SedonaContext.create(spark)

sc = sedona.sparkContext       
         


def main(end_t, begin_t) :
   
   end =datetime.datetime.now() - timedelta(end_t)   
   start=datetime.datetime.now() - timedelta(begin_t)   

   st_dt= start.strftime('%Y-%m-%d')
   end_dt= end.strftime('%Y-%m-%d')
   

   dnn_region = spark.read.option("header", "true").csv("s3a://deep-region/dnn_region.csv")
   dnn_region.createOrReplaceTempView("region")



   out= spark.read.format("parquet").load("s3a://temp/union_data_set.parquet")
   out.createOrReplaceTempView("out_all")

   #딥러닝 지역과 조인 

   region_hps_df = sedona.sql("select collect_dt, collecttime, android_id, msmodel,in_out_none, collecttype, provider, gt_point,\
                                      gt_latitude, gt_longitude, gt_accuracy, gt_dop, gt_hepe, gt_numsat, area_id,  airpress, wificonnapmac ,wifiinfocnt , wifiinfo \
                              from out_all, region \
                              where collecttype = 60 \
                              and (( out_all.gt_latitude * 1000000) between region.latitude_wifi_1 and region.latitude_wifi_2) \
                              and (( out_all.gt_longitude * 1000000) between region.longitude_wifi_1 and region.longitude_wifi_2) \
                              ")

   region_hps_df.createOrReplaceTempView("region_hps")
   region_hps_df.cache()

   ### polygon 지역 추가
   region=["nonhyun1"] 
   polygon = Polygon (
             [ (127.02421 ,37.50447), (127.02735, 37.49744),(127.03724, 37.50058),(127.034, 37.50751) ]
             )
   geodf = gpd.GeoDataFrame(index=region, crs='epsg:4326', geometry=[polygon]) 
   geodf['region'] =geodf.index

   geo_spark =sedona.createDataFrame(geodf)
   geo_spark.createOrReplaceTempView("geodf")

   query = '''
      select collect_dt, collecttime,android_id, msmodel, in_out_none, collecttype, provider ,gt_point,  
             gt_latitude,  gt_longitude , gt_accuracy, gt_dop, gt_hepe, gt_numsat,  b.region as area_id ,   airpress, wificonnapmac,
             wifiinfocnt , wifiinfo
      from out_all a , geodf b
      where st_contains(b.geometry, a.gt_point)
      
    '''

   add_region_df = sedona.sql(query)
   

   region_hps_df_union = add_region_df.unionAll(region_hps_df)

   region_hps_df_union.write.mode("overwrite").parquet("s3a://temp/gt_data_set_loplat.parquet")



if __name__ == "__main__":
   
   try :
       #findspark.init()
       print("start")
       start = time.time()

       main(2 , 30)
       end = time.time()

       print(f"{end - start:.5f} sec")
   except Exception as e:
       print(e)
       spark.stop()
       #spark.close()
   finally :
       spark.stop()