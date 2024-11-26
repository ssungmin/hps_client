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
   
   
   # 1달치의 데이터를 스파크로 read 
   out= spark.read.format("parquet").load("s3a://hps-client/HPS.parquet")
   out.createOrReplaceTempView("hps")

   end =datetime.datetime.now() - timedelta(end_t)   
   start=datetime.datetime.now() - timedelta(begin_t)   

   st_dt= start.strftime('%Y-%m-%d')
   end_dt= end.strftime('%Y-%m-%d')
   
   print(st_dt)
   
   #IQR 방법으로 설정한 값을 필터링 
   query = f"""
       select collect_dt, collecttime,android_id, msmodel, in_out_none, collecttype,  provider,    st_point(cast(gt_longitude as decimal(16,12)) , cast(gt_latitude as decimal(16,12))) as gt_point,
              gt_latitude , gt_longitude, fused_latitude, fused_longitude,  hps_latitude, hps_longitude, 
              gps_latitude, gps_longitude, gps_accuracy, gps_velocity, gps_dop,  gps_hepe, numgps, 
              fused_accuracy, hps_accuracy, float(gt_accuracy) as gt_accuracy , float(gt_velocity) as gt_velocity,  
              float(gt_dop) as gt_dop , float(gt_hepe)  as gt_hepe,  float(gt_numsat)   as gt_numsat , airpress,  wificonnssid as wificonnapmac,
              wifiinfocnt, wifiinfo 
        from hps  
       where (collect_dt between '{st_dt}' and '{end_dt}'
        and ( (in_out_none=2 and gt_dop < 2.55 and gt_accuracy < 21 and gt_numsat > 11 and provider = 2) 
            or (in_out_none=1 and gt_accuracy < 35) 
            or (in_out_none=0 and gt_accuracy < 39) ) and provider <> 0 and float(wifiinfocnt) > 2)
        or ( collect_dt between '{st_dt}' and '{end_dt}' and  collecttype='50' and gt_numsat > 11 and gt_accuracy < 21 and float(wifiinfocnt) > 2)   
    """
   out_df = sedona.sql(query)
   out_df.createOrReplaceTempView("out_view")
   
   # loplat 합치기 

   fSchema = StructType([
    StructField("ADDR_LV1", StringType(), True),
    StructField("ADDR_LV2", StringType(), True),
    StructField("ADDR_LV3", StringType(), True),
    StructField("LOG_TYPE", StringType(), True),
    StructField("TS_LOCAL", StringType(), True),
    StructField("OS_TYPE", StringType(), True),
    StructField("OS_VER", StringType(), True),
    StructField("DEVICE", StringType(), True),
    StructField("PID", StringType(), True),
    StructField("NAME", StringType(), True),      
    StructField("CID", StringType(), True), 
    StructField("COMPLEX_NAME", StringType(), True), 
    StructField("FLOOR", StringType(), True), 
    StructField("ADDR", StringType(), True),
    StructField("LAT", StringType(), True),
    StructField("LNG", StringType(), True),
    StructField("SCANNED_WIFI_STRING", StringType(), True),
    StructField("CONNECTED_WIFI_STRING", StringType(), True),
    StructField("DT", StringType(), True),    
    ])


   lolat= spark.read.option("delimiter",'\u0001').schema(fSchema).load("s3a://loplat/20240410/")
   lolat.createOrReplaceTempView("loplat")
   

   def make_scan_wifi(wifi_in) :
    #print("aa")
     wifiinfo = []
     wifi = wifi_in.split('\t')
    #print(wifi)
     for i in wifi :
        wifiap = i.split(",")
        wifiinfo.append({'apMACAddress': wifiap[0]    , 'apSignalStrength': wifiap[2]   , 'bandWidth':  0  , 'apSSID': wifiap[1]  ,   'rtt': 0    ,'channel': wifiap[3]   })
   
     return wifiinfo
   
   wifiSchema = StructType([
    StructField("apMACAddress", StringType(), True),
    StructField("apSignalStrength", StringType(), True),
    StructField("bandWidth", StringType(), True),
    StructField("apSSID", StringType(), True),
    StructField("rtt", StringType(), True),
    StructField("channel", StringType(), True),
    ])
  
   spark.udf.register("wifiap", make_scan_wifi,  ArrayType(wifiSchema))
  

   query = f"""
          select substring(ts_local,1,10) as collect_dt , ts_local as collecttime, null as android_id, device as msmodel, 2 as in_out_none, 
                 60 as collecttype, null as provider ,   st_point(cast(lng as decimal(16,12)) ,cast(lat as decimal(16,12))) as gt_point,
                 lat as gt_latitude, lng as gt_longitude, null as fused_latitude, null as fused_longitude,  null as hps_latitude, null as hps_longitude, 
                 null as gps_latitude, null as gps_longitude,
                 null as gps_accuracy, null as gps_velocity, null as gps_dop, null as gps_hepe, 
                 null as numgps, null as fused_accuracy , null as hps_accuracy, null as gt_accuracy , null as gt_velocity, 
                 null as gt_dop, null as gt_hepe, null as gt_numsat, null as airpress, null as wificonnapmac,  
                size(split(SCANNED_WIFI_STRING,'\t'))  as wifiinfocnt ,wifiap(SCANNED_WIFI_STRING)  as wifiinfo
          from loplat 
       """
   
   loplat_view = sedona.sql(query)
   loplat_view.createOrReplaceTempView("loplat_view")
   

   ## loplat 데이터 합치기 

   query =f"""
      select  collect_dt, collecttime,android_id, msmodel, in_out_none, collecttype,  provider,    gt_point,
              gt_latitude , gt_longitude, fused_latitude, fused_longitude,  hps_latitude, hps_longitude, 
              gps_latitude, gps_longitude, gps_accuracy, gps_velocity, gps_dop,  gps_hepe, numgps, 
              fused_accuracy, hps_accuracy,  gt_accuracy , gt_velocity,  
               gt_dop ,  gt_hepe,   gt_numsat , airpress,   wificonnapmac,
              wifiinfocnt, wifiinfo 
      from out_view  
      union all
      select  collect_dt, collecttime,android_id, msmodel, in_out_none, collecttype,  provider,    gt_point,
              gt_latitude , gt_longitude, fused_latitude, fused_longitude,  hps_latitude, hps_longitude, 
              gps_latitude, gps_longitude, gps_accuracy, gps_velocity, gps_dop,  gps_hepe, numgps, 
              fused_accuracy, hps_accuracy,  gt_accuracy , gt_velocity,  
               gt_dop ,  gt_hepe,   gt_numsat , airpress,   wificonnapmac,
              wifiinfocnt, wifiinfo 
      from loplat_view 
   """


   out_all_df = sedona.sql(query)
   out_all_df.createOrReplaceTempView("out_all")

   out_all_df.write.mode("overwrite").parquet("s3a://temp/union_data_set.parquet")




   dnn_region = spark.read.option("header", "true").csv("s3a://deep-region/dnn_region.csv")
   dnn_region.createOrReplaceTempView("region")



   out= spark.read.format("parquet").load("s3a://temp/union_data_set.parquet")
   out.createOrReplaceTempView("out_all")

   #딥러닝 지역과 조인 

   region_hps_df = sedona.sql("select collect_dt, collecttime, android_id, msmodel,in_out_none, collecttype, provider, gt_point,\
                               gt_latitude, gt_longitude, gt_accuracy, gt_dop, gt_hepe, gt_numsat, area_id,  airpress, wificonnapmac ,wifiinfocnt , wifiinfo \
                               from out_all, region \
                              where (( out_view.gt_latitude * 1000000) between region.latitude_wifi_1 and region.latitude_wifi_2) \
                              and (( out_view.gt_longitude * 1000000) between region.longitude_wifi_1 and region.longitude_wifi_2) \
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
      from out_view a , geodf b
      where st_contains(b.geometry, a.gt_point)
      
    '''

   add_region_df = sedona.sql(query)
   

   region_hps_df_union = add_region_df.unionAll(region_hps_df)

   region_hps_df_union.write.mode("overwrite").parquet("s3a://temp/gt_data_set.parquet")



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