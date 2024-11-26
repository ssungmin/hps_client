# -*- coding: utf-8 -*-

from pyarrow import csv
import pyarrow.parquet as pq
import pyarrow as pa
#import csv
import pandas as pd
import io
import datetime
#from datetime import datetime
from datetime import date, timedelta
from pyarrow import Table
import re 
import os
import pprint
import pyarrow.orc as orc
import sys
from functools import partial
import time
#bad_lines_fp = open('bad_lines.csv', 'a')
import logging
from minio import Minio
from minio.error import S3Error
from pyarrow import fs, csv, parquet


logger = logging.getLogger("airflow.task")
client = Minio(
    endpoint='172.27.11.56:9000',
    secure=False,
    access_key='minioadmin',
    secret_key='minioadmin'
  )

minio = fs.S3FileSystem(
     endpoint_override='172.27.11.56:9000',
     access_key='minioadmin',
     secret_key='minioadmin',
     scheme='http')

parquet_schema_new = pa.schema([
    ('collect_dt', pa.string()),
    ('idcsstoretime', pa.string()),
    ('android_id', pa.string()),
    ('protocolversion', pa.string()),
    ('hpsclientversion', pa.string()),
    ('msmodel', pa.string()),
    ('servuc_id', pa.string()),
    ('mcc', pa.string()),
    ('nettype', pa.string()),
    ('appid', pa.string()),
    ('collecttype', pa.string()),
    ('groupid', pa.string()),
    ('in_out_none', pa.string()),
    ('provider', pa.string()),
    ('collecttime', pa.string()),
    ('gt_latitude', pa.string()),
    ('gt_longitude', pa.string()),
    ('gt_accuracy', pa.string()),
    ('gt_velocity', pa.string()),
    ('gt_time_stamp', pa.string()),
    ('gt_dop', pa.string()),
    ('gt_hepe', pa.string()),
    ('gt_numsat', pa.string()),
    ('gt_fixtype', pa.string()),
    ('gt_building', pa.string()),
    ('gt_floor', pa.string()),
    ('gt_poi', pa.string()),
    ('gps_latitude', pa.string()),
    ('gps_longitude', pa.string()),
    ('gps_accuracy', pa.string()),
    ('gps_velocity', pa.string()),
    ('gps_time_stamp', pa.string()),
    ('gps_dop', pa.string()),
    ('gps_hepe', pa.string()),
    ('numgps', pa.string()),
    ('fused_latitude', pa.string()),
    ('fused_longitude', pa.string()),
    ('fused_accuracy', pa.string()),
    ('fused_time_stamp', pa.string()),
    ('hps_latitude', pa.string()),
    ('hps_longitude', pa.string()),
    ('hps_accuracy', pa.string()),
    ('hps_time_stamp', pa.string()),
    ('hps_fixtype', pa.string()),
    ('hps_building', pa.string()),
    ('hps_floor', pa.string()),
    ('hps_poi', pa.string()),
    ('airpress', pa.string()),
    ('detectedactivity', pa.string()),
    ('wificonnflag', pa.string()),
    ('wificonnssid', pa.string()),
    ('wificonnrssi', pa.string()),
    ('wificonnlinkspeed', pa.string()),
    ('wifiinfocnt', pa.string()),
    ('wifiinfo', pa.list_(pa.struct([
        ('apMACAddress', pa.string()),
        ('apSignalStrength', pa.string()),
        ('bandWidth', pa.string()),
        ("rtt", pa.string()),   
        ("apSSID", pa.string()),   
        ("channel", pa.string())
    ]))) ,
    ('btinfocnt', pa.string()),
    ('bt_info', pa.list_(pa.struct([
        ('btMACAddress', pa.string()),
        ('btSignalStrength', pa.string()),
        ('btDeviceName', pa.string())
    ]))) ,
    ('magneticDataCnt', pa.int64()),
    ('magnetic_info', pa.list_(pa.struct([
        ('magX', pa.string()),
        ('magY', pa.string()),
        ('magZ', pa.string())
    ]))) ,
    ('cell_infoFlag', pa.string()),
    ('cell_mcc', pa.string()),
    ('cell_mnc', pa.string()),
    ('cell_nettype', pa.string()),
    ('cell_ci', pa.string()),
    ('cell_beamid', pa.string()),
    ('cell_ta', pa.string()),
    ('cell_rsrp', pa.string()),
    ('cell_rsrq', pa.string()),
    ('cell_nr_arfn_downlink', pa.string()),
    ('cell_nr_earfcn_uplink', pa.string()),
    ('cell_band', pa.string()),
    ('cell_rssi', pa.string()),
    ('cell_tx_power', pa.string()),
    ('cell_ri', pa.string()),
    ('cell_rrc', pa.string()),
    ('cell_ip', pa.string()),
    ('cell_cqi', pa.string()),
    ('cell_ca', pa.string()),
    ('cell_s_pci', pa.string()),
    ('cell_s_freq', pa.string()),
    ('cell_s_bandwidth', pa.string()),
    ('cell_s_rsrp', pa.string()),
    ('cell_s_rsrq', pa.string()),
    ('cell_s_sinr', pa.string()),
    ('cell_s_beamid', pa.string()),
    ('cell_s_ta', pa.string()),
    ('cell_s2_ca', pa.string()),
    ('cell_s2_pci', pa.string()),
    ('cell_s2_freq', pa.string()),
    ('cell_s2_bandwidth', pa.string()),
    ('cell_s2_rsrp', pa.string()),
    ('cell_s2_rsrq', pa.string()),
    ('cell_s2_sinr', pa.string()),
    ('cell_s2_beamid', pa.string()),
    ('cell_s2_ta', pa.string()),
    ('cell_s3_ca', pa.string()),
    ('cell_s3_pci', pa.string()),
    ('cell_s3_freq', pa.string()),
    ('cell_s3_bandwidth', pa.string()),
     ('cell_s3_rsrp', pa.string()),
    ('cell_s3_rsrq', pa.string()),
    ('cell_s3_sinr', pa.string()),
    ('cell_s3_baemid', pa.string()),
    ('cell_MrleCnt', pa.string()),
    ('cell_ref_info', pa.list_(pa.struct([
        ('beamid', pa.string()),
        ('pci', pa.string()),
        ('rsrp', pa.string()),
        ("rsrq", pa.string()),
        ("freq", pa.string()),
        ("sinr", pa.string())
    ])))
])  




def write_bad_line(line, fp, sep='|'):
    fp.write(sep.join(line) + '\n')
    return None  # return None to skip the line while processing


def validate_ap_mac_address(mac_addresses):
    for mac_address in mac_addresses:
        if not re.match('^([0-9A-Fa-f]{2}[:-]){5}([0-9A-Fa-f]{2})$', mac_address):
            return False
    return True

def check_wifi_data_type(splited_row):

    # 1. 
    if (len(splited_row) < 56):
        #print(f"Short line : {str(len(splited_row))}")
        return False
    
    # 2. 
    wifi_info_cnt = int(splited_row[54])
    if ( wifi_info_cnt < 0 or (len(splited_row) < (55 + (wifi_info_cnt*6)))):
        #print(f"Invalid wifi info cnt : {wifi_info_cnt}")
        return False

    # 3. 
    ap_mac_addresses = [splited_row[55 + 6 * i] for i in range(wifi_info_cnt)]
    if not validate_ap_mac_address(ap_mac_addresses):
        #print(f"Invalid MAC address format")
        return False

    return True

def remove_non_ascii(text):
    text = text.replace('"', '')    
    return ''.join(i for i in text if ord(i) < 128)


def convert_unixtime(date_time):

    import datetime    

    unixtime = datetime.datetime.strptime(date_time, '%Y-%m-%d %H:%M:%S,%f').timestamp()
    return unixtime

def makeDataFrame(lines):

    list_row = []
    wifiinfo=[]
    btinfo =[]
    magneticData=[]  
    cell_ref_info=[]

    magneticDataCnt = 0
    time_1='2024-01-01 00:00:00'

    android_id=''
    protocol_version=''
    hpsclient_version=''
    msmodel =''
    servuc_id =''
    mcc =''
    nettype=''
    appid =''
    type=''
    group_id=''
    in_out = ''
    provider =''
    time_stamp = ''
    gt_latitude = ''
    gt_longitude =''
    gt_accuracy = ''
    gt_velocity =''
    gt_timestamp = ''
    gt_dop = ''
    gt_hepe= ''
    gt_numsat =''
    gt_fixtype=''
    gt_building =''
    gt_floor =''
    gt_poi = ''
    gps_latitude = ''
    gps_longitude = ''
    gps_accuracy = ''
    gps_velocity = ''
    gps_timestamp = ''
    gps_dop = ''
    gps_hepe = ''
    numgps =''
    fused_latitude =''
    fused_longitude =''
    fused_accuracy = ''
    fused_time_stamp =''
    hps_latitude = ''
    hps_longitude = ''
    hps_accuracy = ''
    hps_time_stamp =''
    hps_fixtype =''
    hps_building = ''
    hps_floor = ''
    hps_poi = ''
    airpress = ''
    detectedactivity = ''
    wificonnflag= ''
    wificonnssid = ''
    wificonnApMac = ''
    whficonnCh = ''
    wificonnRssi = ''
    wificonnlinkSpeed = ''
    wifiinfoCnt = ''

    cell_mcc = ""
    cell_mnc = ""
    cell_nettype =""
    cell_ci = ""
    cell_beamid = ""
    cell_ta= ""
    cell_pci = ""
    cell_tac = ""
    cell_rsrp = ""
    cell_rsrq= ""
    cell_nr_arfn_downlink = ""
    cell_nr_earfcn_uplink=""
    cell_band = ""
    cell_bandwidth = ""
    cell_rssi = ""
    cell_tx_power = ""
    cell_sinr = ""
    cell_ri = ""
    cell_rrc =""
    cell_ip = ""
    cell_cqi =""
    cell_ca = ""
    cell_s_pci = ""
    cell_s_freq = ""
    cell_s_bandwidth = ""
    cell_s_rsrp = ""
    cell_s_rsrq = ""
    cell_s_sinr = ""
    cell_s_beamid = ""
    cell_s_ta = ""
    cell_s2_ca = ""
    cell_s2_pci= ""
    cell_s2_freq= ""
    cell_s2_bandwidth=""
    cell_s2_rsrp = ""
    cell_s2_rsrq = ""
    cell_s2_sinr = ""
    cell_s2_beamid = ""
    cell_s2_ta = ""
    cell_s3_ca = ""
    cell_s3_pci = ""
    cell_s3_freq = ""
    cell_s3_bandwidth = ""
    cell_s3_rsrp = ""
    cell_s3_rsrq = ""
    cell_s3_sinr = ""
    cell_s3_baemid = ""
    cell_s3_ta = ""

    cell_MrleCnt = ""

    for i, line in enumerate(lines):
        #print("** csv_file:", csv_file)
        #티맵 데이터는 , 로 구별
        rows= line.strip().split(',') 
        #print (rows)
        cleaned_row = [remove_non_ascii(str(cell)) for cell in rows]
        #rows= line.strip().split(',') 
               
        if rows[0] == '1' :
            
            wifiinfo=[]
            btinfo =[]
            magneticData=[]  
            cell_ref_info=[]
            magneticDataCnt = 0
            
            time_1 = rows[7]
            android_id=''
            protocol_version=''
            hpsclient_version=''
            msmodel =''
            servuc_id =''
            mcc =''
            nettype=''
            appid =''
            type=''
            group_id=''
            in_out = '2'
            provider ='2'
            time_stamp = ''
            gt_latitude = rows[1]
            gt_longitude =rows[2]
            gt_accuracy = ''
            gt_velocity =''
            gt_timestamp = ''
            gt_dop = ''
            gt_hepe= ''
            gt_numsat =rows[3]
            gt_fixtype=''
            gt_building =''
            gt_floor =''
            gt_poi = ''
            gps_latitude = ''
            gps_longitude = ''
            gps_accuracy = ''
            gps_velocity = ''
            gps_timestamp = ''
            gps_dop = ''
            gps_hepe = ''
            numgps =''
            fused_latitude =''
            fused_longitude =''
            fused_accuracy = ''
            fused_time_stamp =''
            hps_latitude = ''
            hps_longitude = ''
            hps_accuracy = ''
            hps_time_stamp =''
            hps_fixtype =''
            hps_building = ''
            hps_floor = ''
            hps_poi = ''
            airpress = ''
            detectedactivity = ''
            wificonnflag= ''
            wificonnssid = ''
            wificonnApMac = ''
            whficonnCh = ''
            wificonnRssi = ''
            wificonnlinkSpeed = ''
            wifiinfoCnt = rows[6]

            cell_mcc = ""
            cell_mnc = ""
            cell_nettype =""
            cell_ci = ""
            cell_beamid = ""
            cell_ta= ""
            cell_pci = ""
            cell_tac = ""
            cell_rsrp = ""
            cell_rsrq= ""
            cell_nr_arfn_downlink = ""
            cell_nr_earfcn_uplink=""
            cell_band = ""
            cell_bandwidth = ""
            cell_rssi = ""
            cell_tx_power = ""
            cell_sinr = ""
            cell_ri = ""
            cell_rrc =""
            cell_ip = ""
            cell_cqi =""
            cell_ca = ""
            cell_s_pci = ""
            cell_s_freq = ""
            cell_s_bandwidth = ""
            cell_s_rsrp = ""
            cell_s_rsrq = ""
            cell_s_sinr = ""
            cell_s_beamid = ""
            cell_s_ta = ""
            cell_s2_ca = ""
            cell_s2_pci= ""
            cell_s2_freq= ""
            cell_s2_bandwidth=""
            cell_s2_rsrp = ""
            cell_s2_rsrq = ""
            cell_s2_sinr = ""
            cell_s2_beamid = ""
            cell_s2_ta = ""
            cell_s3_ca = ""
            cell_s3_pci = ""
            cell_s3_freq = ""
            cell_s3_bandwidth = ""
            cell_s3_rsrp = ""
            cell_s3_rsrq = ""
            cell_s3_sinr = ""
            cell_s3_baemid = ""
            cell_s3_ta = ""

            cell_MrleCnt = ""
            cell_ref_info.append({'beamid':'', 'pci':'', 'rsrp':'','rsrq': '','freq':'', 'sinr':''})  
            print("row=" + "1")
            print("lat=" + gt_latitude)
            print("lon=" + gt_longitude)

        elif rows[0] == '2' :
            servuc_id = rows[1]
            nettype= rows[2]
            print("row=" + "2")
            print("lat=" + gt_latitude)
            print("lon=" + gt_longitude)

        #elif rows[0] == 3 :

        elif rows[0]  == '4' :
            #print(rows)
            apMACAddress = rows[1]
            apSignalStrength = rows[3]
            bandWidth = ''
            rtt = rows[6]
            apSSID = rows[2]
            channel = int(rows[5]) + 1
             # channel 값 0이면 1 로 1이면 2로 변환
            wifiinfo.append({'apMACAddress':str(apMACAddress),'apSignalStrength':str(apSignalStrength), 'bandWidth':str(bandWidth),'rtt':str(rtt), 'apSSID':str(apSSID),'channel':str(channel)})
            #print(wifiinfo)
            btinfo.append({'btMACAddress':'','btSignalStrength':'', 'btDeviceName':''})

            print("row=" + "4")
            print("lat=" + gt_latitude)
            print("lon=" + gt_longitude)
                #btinfo.append({})
        elif rows[0]  == '6' :
            airpress = rows[1]

            print("row=" + "6")
            print("lat=" + gt_latitude)
            print("lon=" + gt_longitude)
        elif rows[0] ==  '8' :
            gt_accuracy= rows[1]
            print("row=" + "8")
            print("lat=" + gt_latitude)
            print("lon=" + gt_longitude)

        elif rows[0] =='M4' :
             magneticDataCnt = magneticDataCnt + 1   
             magX = rows[1]
             magY=  rows[2]
             magZ = rows[3]
             
             print("row=" + "m4")
             print("lat=" + gt_latitude)
             print("lon=" + gt_longitude)
             magneticData.append({'magX':str(magX),'magY':str(magY), 'magZ':str(magZ)})
        
        
        elif rows[0] == 'M9' :
            print("m9 start")

            if len(rows) > 6 :
                wificonnflag= rows[1]
                wificonnssid = rows[3]
                wificonnApMac = rows[2]
                whficonnCh = rows[4]
                wificonnRssi = rows[5]
                wificonnlinkSpeed = rows[6]

            pd.set_option('display.max_columns', None)
        
            time_2 = datetime.datetime.strptime(time_1, '%Y-%m-%d %H:%M:%S')
        
        #time_2 = datetime.datetime.fromtimestamp(int(time_1))
       
            collect_dt = time_2.strftime('%Y-%m-%d')
            #print(collect_dt)
            #timestamp_2 = datetime.datetime.combine(time_2, datetime.datetime.min.time()).timestamp
           # print(str(timestamp_2))

            #print(time.mktime(datetime.datetime.strptime(time_1, '%Y-%m-%d %H:%M:%S').timetuple()))

            idcs_time = int(time.mktime(datetime.datetime.strptime(time_1, '%Y-%m-%d %H:%M:%S').timetuple()))
            record = {}
            record['collect_dt'] = collect_dt
            record['idcsstoretime']=str(idcs_time)
            record['android_id'] = android_id
            record['protocolversion'] = protocol_version
            record['hpsclientversion'] = hpsclient_version
            record['msmodel'] = msmodel
            record['servuc_id'] = servuc_id
            record['mcc'] = mcc
            record['nettype'] = nettype
            record['appid'] = appid
            record['collecttype']="50"
            record['groupid'] = group_id
            record['in_out_none'] = in_out
            record['provider'] = provider
            record['collecttime'] = str(idcs_time)
            record['gt_latitude'] = gt_latitude
            record['gt_longitude'] = gt_longitude
            record['gt_accuracy'] = gt_accuracy
            record['gt_velocity']=gt_velocity
            record['gt_time_stamp']=gt_timestamp
            record['gt_dop']=gt_dop
            record['gt_hepe']=gt_hepe
            record['gt_numsat']=gt_numsat        
            record['gt_fixtype']=gt_fixtype              
            record['gt_building'] = gt_building
            record['gt_floor'] = gt_floor
            record['gt_poi']=gt_poi

            record['gps_latitude'] = gps_latitude
            record['gps_longitude'] = gps_longitude
            record['gps_accuracy'] = gps_accuracy
            record['gps_velocity'] = gps_velocity
            record['gps_time_stamp'] = gps_timestamp
            record['gps_dop'] = gps_dop
            record['gps_hepe'] = gps_hepe
            record['numgps'] = numgps

            record['fused_latitude'] = fused_latitude
            record['fused_longitude']=fused_longitude
            record['fused_accuracy'] = fused_accuracy
            record['fused_time_stamp'] = fused_time_stamp

            record['hps_latitude'] = hps_latitude
            record['hps_longitude'] = hps_longitude
            record['hps_accuracy'] = hps_accuracy
            record['hps_time_stamp'] = hps_time_stamp
            record['hps_fixtype'] = hps_fixtype
            record['hps_building'] = hps_building
            record['hps_floor'] = hps_floor
            record['hps_poi'] = hps_poi
            record['airpress'] = airpress
            record['detectedactivity'] = detectedactivity
        
            record['wificonnflag'] = wificonnflag
            record['wificonnssid'] = wificonnssid
            record['wificonnapmac'] = wificonnApMac
            record['whficonnCh'] = whficonnCh
            record['wificonnrssi'] = wificonnRssi
            record['wificonnlinkspeed'] = wificonnlinkSpeed

            record['wifiinfocnt'] = wifiinfoCnt
            record['wifiinfo'] = wifiinfo
            record['btinfocnt'] = '0'
            record['bt_info'] = btinfo
            record['magneticDataCnt'] = magneticDataCnt
            record['magnetic_info'] = magneticData

            record['cell_infoFlag']='0'

            record['cell_mcc'] = cell_mcc
            record['cell_mnc'] = cell_mnc
            record['cell_nettype'] = cell_nettype
            record['cell_ci'] = cell_ci
            record['cell_beamid'] = cell_beamid
            record['cell_ta'] = cell_ta
            record['cell_pci']=cell_pci
            record['cell_tac'] = cell_tac
            record['cell_rsrp'] = cell_rsrp
            record['cell_rsrq'] = cell_rsrq
            record['cell_nr_arfn_downlink'] = cell_nr_arfn_downlink
            record['cell_nr_earfcn_uplink'] = cell_nr_earfcn_uplink
            record['cell_band'] = cell_band
            record['cell_bandwidth'] = cell_bandwidth
            record['cell_rssi'] = cell_rssi
            record['cell_tx_power'] = cell_tx_power
            record['cell_sinr'] = cell_sinr
            record['cell_ri'] = cell_ri
            record['cell_rrc'] = cell_rrc
            record['cell_ip'] = cell_ip
            record['cell_cqi'] = cell_cqi
            record['cell_ca'] = cell_ca

            record['cell_s_pci']=cell_s_pci
            record['cell_s_freq'] = cell_s_freq
            record['cell_s_bandwidth'] = cell_s_bandwidth
            record['cell_s_rsrp'] = cell_s_rsrp
            record['cell_s_rsrq'] = cell_s_rsrq
            record['cell_s_sinr'] = cell_s_sinr
            record['cell_s_beamid']=cell_s_beamid
            record['cell_s_ta'] = cell_s_ta

            record['cell_s2_ca'] = cell_s2_ca
            record['cell_s2_pci'] = cell_s2_pci
            record['cell_s2_freq'] = cell_s2_freq
            record['cell_s2_bandwidth'] = cell_s2_bandwidth
            record['cell_s2_rsrp'] = cell_s2_rsrp
            record['cell_s2_rsrq'] = cell_s2_rsrq
            record['cell_s2_sinr'] = cell_s2_sinr
            record['cell_s2_beamid'] = cell_s2_beamid
            record['cell_s2_ta'] = cell_s2_ta

            record['cell_s3_ca'] = cell_s3_ca
            record['cell_s3_pci'] = cell_s3_pci
            record['cell_s3_freq'] = cell_s3_freq
            record['cell_s3_bandwidth'] = cell_s3_bandwidth
            record['cell_s3_rsrp'] = cell_s3_rsrp
            record['cell_s3_rsrq'] = cell_s3_rsrq
            record['cell_s3_sinr'] = cell_s3_sinr
            record['cell_s3_baemid'] = cell_s3_baemid
            record['cell_s3_ta'] = cell_s3_ta
            record['cell_MrleCnt'] = cell_MrleCnt
            record['cell_ref_info'] = cell_ref_info
       
            print(record)
            list_row.append(record)
            
            print("row=" + "m9")
            print("lat=" + gt_latitude)
            print("lon=" + gt_longitude)

    df = pd.DataFrame(list_row)
    print(df)
    return df
                         

def read_tmap(csv_file) :

    df = ''
    with open(csv_file, 'r', encoding='utf-8', errors='replace') as file:
        lines = file.readlines()
        logger.info("** csv_file:", csv_file)
        try: 
            df = makeDataFrame(lines)
        #print (df)
        except Exception as e:
            #print(cleaned_row)
            logger.error(e)
            #logger.error(f"{str(i)} Parse fail ")
            #pass

    table = Table.from_pandas(df, schema=parquet_schema_new)   
    #pq.write_to_dataset(table ,"/Users/1111764/Documents/location/development/TIDC/TIDC/hps_client/parsing/HPS.parquet",compression='GZIP',partition_cols=['collect_dt','collecttype'],use_dictionary=True)
    pq.write_to_dataset(table ,"/Users/1111764/Documents/location/development/TIDC/TIDC/hps_client/parsing/HPS.parquet",compression='GZIP',partition_cols=['collect_dt','collecttype'],use_dictionary=True)
    
    
   # parquet_load(table)


def parquet_load(table) :
    try :
       found = client.bucket_exists("hps-client")
       if not found:
          client.make_bucket("hps-client")
       #else:
       #  logger.error("Bucket 'test' already exists")

       #now =datetime.datetime.now() - timedelta(1)   
       #today_datetime = now.strftime('%Y%m%d')

       pq.write_to_dataset(
       table=table,
       root_path="hps-client/HPS.parquet",
       filesystem=minio,
       compression='GZIP',
       partition_cols=['collect_dt','collecttype'],
       use_dictionary=True)

    except Exception as e:    
       logger.error(e)     

def combine_parquet_files(input_folder, target_path):
    try:
        files = []
        for file_name in os.listdir(input_folder):
            files.append(pq.read_table(os.path.join(input_folder, file_name)))
        with pq.ParquetWriter(target_path,
                files[0].schema,
                version='2.0',
                compression='gzip',
                use_dictionary=True,
                data_page_size=2097152, #2MB
                write_statistics=True) as writer:
            for f in files:
                writer.write_table(f)
    except Exception as e:
        print(e) 

def main(dt):
#dir_path = '/DATA/pms_app/PMS_DATA'
   
    #dir_path= os.path.join(dir_path, sys.argv[1])
    #dir_path= os.path.join(dir_path, "/WIFI_RAWDATA/LOG")
    ## 날짜는 외부 변수로 받자 
    
    file_name="/Users/1111764/Documents/location/development/TIDC/TIDC/hps_client/parsing/sample/TMAP_wifidata_2024041507.txt"
    
    
    
    if os.stat(file_name).st_size > 0:
                            #print(f_name)
        read_tmap(file_name)                         

def read_parquet() :

    df = pd.read_parquet('/Users/1111764/Documents/location/development/TIDC/TIDC/hps_client/parsing/HPS.parquet', engine='pyarrow') 
    print(df[['in_out_none','gt_latitude','gt_longitude','gt_hepe','gt_hepe','airpress','wifiinfocnt','wifiinfo','bt_info']])


if __name__ == "__main__":

   #print(sys.argv[1])
   main(sys.argv[1] )
   #read_parquet()


docker run -p 5432:5432 -d  --name airflow-meta  -e POSTGRES_PASSWORD=posggres -e TZ=Asia/Seoul -v /home/svcapp_su/pgsql/data:/var/lib/postgresql/data postgres:15.2