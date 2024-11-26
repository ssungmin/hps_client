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

findspark.init()
spark = SparkSession.builder. \
        master("spark://172.27.11.56:7077").\
        appName("filteringData").\
        config("spark.driver.memory","50g").\
        config("spark.executor.memory","20g").\
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
         getOrCreate()

sedona = SedonaContext.create(spark)

sc = sedona.sparkContext       
         

def main(end_t, begin_t) :
   
   end =datetime.datetime.now() - timedelta(end_t)   
   start=datetime.datetime.now() - timedelta(begin_t)   

   st_dt= start.strftime('%Y-%m-%d')
   end_dt= end.strftime('%Y-%m-%d')
   # 1달치의 데이터를 스파크로 read 
   data= spark.read.format("parquet").load("s3a://temp/gt_data_set.parquet")
   data.createOrReplaceTempView("data")

   

   explode_df = data.select ( "collect_dt", "collecttime","android_id", "msmodel", "in_out_none", "collecttype", "provider" , "gt_point" , \
                                           "gt_latitude",  "gt_longitude" , "gt_accuracy", "gt_dop", "gt_hepe", "gt_numsat" , "airpress", "wificonnssid","wificonnapmac" , \
                                           "area_id","wifiinfocnt" , explode(data.wifiinfo).alias("wifi")  \
                                           )
   
   explode_df = explode_df.select("collect_dt", "collecttime","android_id", "msmodel", "in_out_none", "collecttype", "provider" , "gt_point" , \
                                 "gt_latitude",  "gt_longitude" , "gt_accuracy", "gt_dop", "gt_hepe", "gt_numsat" ,"airpress" , "wificonnssid","wificonnapmac" ,\
                                 "area_id","wifiinfocnt" , "wifi.*"
                                 )
   # 이동형 AP 이름으로 필터링
   remove_keyword=['vacumn', 'waterpurify', 'airpurifier','ggbus','android', 'hotspot', 'iphone', 'galaxy', 'xiaomi', 'huawei', 'oppo','dashcam', 'audi', 'bmw', 'socar', 'mayton', 'porsche', 'kia', 'cayenne', 'chevrolet','roaming', 'tpocket', 'ktegg']
   
   regex_values ="|".join(remove_keyword)
   
   explode_df = explode_df.withColumn('apSSID_lower', lower('apSSID'))
   

   #out_df.write.mode("overwrite").parquet("s3a://temp/month.parquet")
   explode_df_filter = explode_df.filter(~explode_df.apSSID_lower.rlike(regex_values))
  
   explode_df_filter.createOrReplaceTempView("explode_df_filter")


   #ap별 중심점 구하기

   zip_lists = f.udf(lambda x, y: MultiPoint([Point(z) for z in zip(x,y)]).wkt, StringType())

   query = '''
         select area_id, apMACAddress, apSSID, collect_list(gt_latitude) as lat_list , collect_list(gt_longitude) as lon_list , count(*) as total ,
                sum(case when datediff(to_date(''' + end_dt + '''), to_date(collect_dt)) <=5 then 1 end ) as 5dayin ,
                sum(case when datediff(to_date(''' + end_dt + '''), to_date(collect_dt)) > 5 then 1 end ) as 5dayout 
         from explode_df_filter 
         group by  area_id, apMACAddress, apSSID          
   '''

   multipoint_df = sedona.sql(query)
   
   result_df = multipoint_df.select("area_id", "apMACAddress" , "apSSID", "lat_list", "lon_list", "total","5dayin", "5dayout", zip_lists(multipoint_df.lon_list,multipoint_df.lat_list).alias('points'))

   result_df.createOrReplaceTempView("result_df")

   # area_id", "apMACAddress" , "apSSID", "lat_list", "lon_list", "total","5dayin", "5dayout , points
   ap_center_df  =   sedona.sql("select * , st_centroid(st_geomfromtext(points)) as center from result_df where total >= 10")   
   ap_center_df.cache()
   ap_center_df.createOrReplaceTempView("ap_center_df")
   ## ap 필터링

   query = '''
           select df.collect_dt, df.collecttime, df.android_id, df.msmodel, df.in_out_none, df.collecttype, df.provider, df.gt_latitude, df.gt_longitude, df.gt_accuracy, 
                  df.gt_dop, df.gt_hepe, df.gt_numsat , df.airpress, df.wificonnssid,df.wificonnapmac, df.area_id, df.wifiinfocnt, df.apMACAddress, df.apSSID ,  df.apSignalStrength , df.bandWidth , df.rtt ,df.channel, cen.center
           from explode_df_filter df
           inner join ap_center_df cen
           on df.area_id = cen.area_id and df.apMACAddress = cen.apMACAddress and df.apSSID = cen.apSSID
   '''
   data_set  =   sedona.sql(query)   
  

   data_set.write.mode("overwrite").parquet("s3a://temp/data_set.parquet")


   # dnn data set 지역별로 만들기 


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