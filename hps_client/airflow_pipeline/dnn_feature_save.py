import findspark
import pandas as pd
import sys
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
from sedona.spark import *
from pyspark.sql.functions import lower, upper
from pyspark.sql.functions import expr, collect_list, arrays_zip
from pyspark.sql.types import *
import pyspark.sql.functions as f


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
        config("spark.task.maxFailures", "8"). \
        config("spark.stage.maxConsecutiveAttempts", "8"). \
        config("spark.network.timeout", "800s"). \
        config("spark.rpc.askTimeout", "600s"). \
        config("spark.shuffle.io.connectionTimeout", "600s"). \
        config("spark.locality.wait", "3s"). \
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

   start=datetime.datetime.now() - timedelta(2)   

   dt= start.strftime('%Y-%m-%d')
   
   out =sedona.read.format("parquet").load("s3a://temp/mv_data_set.parquet")

   out.createOrReplaceTempView("data") 
   
   query = f"""
          select * from data 
       """
   out_df = sedona.sql(query)
   out_df.createOrReplaceTempView("view")
   query =f"""
      select area_id , dense_rank() over (partition by area_id  order by gt_latitude,gt_longitude , gt_accuracy,collecttime,gt_numsat, airpress , wifiinfocnt, in_out_none, wifiConnApMAC) as tp_id ,  
         dense_rank() over (partition by area_id  order by apMACAddress) as ap_id , collect_dt, collecttime, android_id, msmodel, in_out_none, collecttype,
         provider, gt_latitude, gt_longitude, int(gt_accuracy), int(gt_numsat), bigint(airpress), wificonnssid, wificonnapmac, int(wifiinfocnt), apMACAddress, apssid, apsignalstrength, channel, bandwidth
   from view 

   """

   tp_df = sedona.sql(query)
   tp_df.createOrReplaceTempView("tp_df")


   ### ap 


   query =f"""
      select  area_id, ap_id, apMACAddress, max(apSSID) as apSSID, max(bandWidth) as bandWidth
      from tp_df 
      group by area_id, ap_id, apMACAddress

      """
   ap_df = sedona.sql(query)

   ap_df.write.format("parquet") \
      .mode("overwrite") \
      .partitionBy("area_id") \
      .option("header","true") \
      .save(f"s3a://dnn-data-set/{dt}/ap")

   ap_df.drop()

   query = f"""
         select * , case when tp_cnt >= 0.8  then 'validation' else 'training' end  as gubun
         from (
            select * , row_number() over (partition by area_id order by rand()) / count(1) over (partition by area_id) as tp_cnt
            from (select distinct area_id,tp_id, gt_latitude,gt_longitude , gt_accuracy,collecttime,gt_numsat, airpress , wifiinfocnt, in_out_none, wificonnssid,wifiConnApMAC , collecttype,msmodel
                  from tp_df ) temp
            ) temp2
         """

   tp_gubun = sedona.sql(query)
   tp_gubun.cache()
   tp_gubun.createOrReplaceTempView("tp_gubun")


   query =f"""
      select  area_id, tp_id, dense_rank() over (partition by area_id  order by gt_latitude,gt_longitude , gt_accuracy,collecttime,gt_numsat, airpress , wifiinfocnt, in_out_none, wifiConnApMAC) as new_tp_id ,  gt_latitude,gt_longitude,gt_accuracy,collecttime,gt_numsat,airpress,wifiinfocnt,in_out_none,wificonnssid,wifiConnApMAC,collecttype, msmodel
      from tp_gubun
      where gubun='training'
      order by area_id, tp_id
      """

   tp_train = sedona.sql(query)
   #tp_train.persist(StorageLevel.MEMORY_AND_DISK)
   tp_train.createOrReplaceTempView("tp_train")

   tp_train.write.format("parquet") \
   .mode("overwrite") \
   .partitionBy("area_id") \
   .option("header","true") \
   .save(f"s3a://dnn-data-set/{dt}/train/tp")


   query =f"""
       select area_id, tp_id, dense_rank() over (partition by area_id  order by gt_latitude,gt_longitude , gt_accuracy,collecttime,gt_numsat, airpress , wifiinfocnt, in_out_none, wifiConnApMAC) as new_tp_id ,  gt_latitude,gt_longitude,gt_accuracy,collecttime,gt_numsat,airpress,wifiinfocnt,in_out_none,wificonnssid,wifiConnApMAC,collecttype,msmodel
      from tp_gubun
      where gubun='validation'
      order by area_id, tp_id
   """

   tp_validatioin = sedona.sql(query)
   #tp_validatioin.persist(StorageLevel.MEMORY_AND_DISK)
   tp_validatioin.createOrReplaceTempView("tp_validation")

   tp_validatioin.write.format("parquet") \
   .mode("overwrite") \
   .partitionBy("area_id") \
   .option("header","true") \
   .save(f"s3a://dnn-data-set/{dt}/validation/tp")

   tp_validatioin.createOrReplaceTempView("tp_validation")
   tp_gubun.drop()
   # train RSSI 

   query = f"""
      select a.area_id, b.new_tp_id, a.ap_id, a.apMACAddress, a.apsignalstrength
      from tp_df a
      inner join tp_train b
      on a.area_id = b.area_id and a.tp_id = b.tp_id
      order by b.tp_id
      """
   train_rssi = sedona.sql(query)
   
   train_rssi.repartition("area_id") \
   .write.format("parquet") \
   .mode("overwrite") \
   .partitionBy("area_id") \
   .save(f"s3a://dnn-data-set/{dt}/train/rssi")

   tp_train.drop()
   train_rssi.drop()
   # validation RSSI

   query = f"""
      select a.area_id, b.new_tp_id, a.ap_id, a.apMACAddress,a.apsignalstrength
      from tp_df a
      inner join tp_validation b
      on a.area_id = b.area_id and a.tp_id = b.tp_id
      order by b.tp_id
      """
   validation_rssi = sedona.sql(query)
   validation_rssi.repartition("area_id").write.format("parquet") \
   .mode("overwrite") \
   .partitionBy("area_id") \
   .save(f"s3a://dnn-data-set/{dt}/validation/rssi")
   
   tp_validatioin.drop()  
   validation_rssi.drop()

   region = sedona.read.option("header","true").csv("s3a://deep-region/dnn_region.csv")
   region.createOrReplaceTempView("region")
   region_df = sedona.sql("select area_id, latitude_wifi_1/1000000, longitude_wifi_1 / 1000000 from region where use_yn='Y'")

   region_df.write.format("parquet") \
   .mode("overwrite") \
   .partitionBy("area_id") \
   .save(f"s3a://dnn-data-set/{dt}/train/MIN")

   region_df.drop()

   #tp_gubun.drop()
   #tp_train.drop()
   #tp_validatioin.drop()







if __name__ == "__main__":

   try :
      findspark.init()
      start = time.time()
      main()
      end = time.time()

      print(f"{end - start:.5f} sec")
   except Exception as e:
      print(e)
      spark.stop()
      #spark.close()

   finally :
      spark.stop()
      #spark.close()
 