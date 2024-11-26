# -*- coding: utf-8 -*-

from pyarrow import csv
import pyarrow.parquet as pq
import pyarrow as pa
#import csv
import pandas as pd
import io
import datetime
from pyarrow import Table
import re 
import os
import pprint
import pyarrow.orc as orc
import sys
from functools import partial
import time
bad_lines_fp = open('bad_lines.csv', 'a')



parquet_schema_new = pa.schema([
    ('collect_dt', pa.string()),
    ('idcsstoretime', pa.string()),
    ('android_id', pa.string()),
    ('protocolversion', pa.string()),
    ('hpsclientversion', pa.string()),
    ('msmodel', pga.string()),
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
        print(f"Short line : {str(len(splited_row))}")
        return False
    
    # 2. 
    wifi_info_cnt = int(splited_row[54])
    if ( wifi_info_cnt < 0 or (len(splited_row) < (55 + (wifi_info_cnt*6)))):
        print(f"Invalid wifi info cnt : {wifi_info_cnt}")
        return False

    # 3. 
    ap_mac_addresses = [splited_row[55 + 6 * i] for i in range(wifi_info_cnt)]
    if not validate_ap_mac_address(ap_mac_addresses):
        print(f"Invalid MAC address format")
        return False

    return True

def remove_non_ascii(text):
    text = text.replace('"', '')    
    return ''.join(i for i in text if ord(i) < 128)

def parse_file(file_path):
    # print(file_path)

    hps_client_data_list = []
    
    with open(file_path, 'r', encoding='utf-8', errors='replace') as file:
        lines = file.readlines()
        
    for i, line in enumerate(lines):
        # print(f"{i} line parsing")
        splited_row = line.strip().split('|')
        cleaned_row = [remove_non_ascii(str(cell)) for cell in splited_row]
        
        try: 
            if check_wifi_data_type(cleaned_row):
                hps_client_row = HPSClientData(cleaned_row)
                hps_client_data_list.append(hps_client_row.__dict__)
        except:
            print(f"{str(i)} Parse fail ")
            pass

    print(f"{file_path} parse done")
    hps_client_df = pd.DataFrame(hps_client_data_list)

    return hps_client_df   

def convert_unixtime(date_time):

    import datetime    

    unixtime = datetime.datetime.strptime(date_time, '%Y-%m-%d %H:%M:%S,%f').timestamp()
    return unixtime

def makeRow(rows):
      # print(row[54])
        time_1 = rows[0]
        android_id=rows[1]
        protocol_version=rows[2]
        hpsclient_version=rows[3]
        msmodel =rows[4]
        servuc_id =rows[5]
        mcc =rows[6]
        nettype=rows[7]
        appid =rows[8]
        type=rows[9]
        group_id=rows[10]
        in_out = rows[11]
        provider =rows[12]
        time_stamp = rows[13]
        gt_latitude =rows[14]
        gt_longitude =rows[15]
        gt_accuracy = rows[16]
        gt_velocity =rows[17]
        gt_timestamp = rows[18]
        gt_dop = rows[19]
        gt_hepe= rows[20]
        gt_numsat =rows[21]
        gt_fixtype=rows[22]
        gt_building =rows[23]
        gt_floor =rows[24]
        gt_poi = rows[25]
        gps_latitude = rows[26]
        gps_longitude = rows[27]
        gps_accuracy = rows[28]
        gps_velocity = rows[29]
        gps_timestamp = rows[30]
        gps_dop = rows[31]
        gps_hepe = rows[32]
        numgps =rows[33]
        fused_latitude =rows[34]
        fused_longitude =rows[35]
        fused_accuracy = rows[36]
        fused_time_stamp =rows[37]
        hps_latitude = rows[38]
        hps_longitude = rows[39]
        hps_accuracy = rows[40]
        hps_time_stamp =rows[41]
        hps_fixtype =rows[42]
        hps_building = rows[43]
        hps_floor = rows[44]
        hps_poi = rows[45]
        airpress = rows[46]
        detectedactivity = rows[47]
        wificonnflag= rows[48]
        wificonnssid = rows[49]
        wificonnApMac = rows[50]
        whficonnCh = rows[51]
        wificonnRssi = rows[52]
        wificonnlinkSpeed = rows[53]
        wifiinfoCnt = rows[54]

        wifiinfo=[]
        wifinfo_start = 54
        wifinfo_end=wifinfo_start
        #print("andord_id:=%s"%android_id)
        #print("gps_latitude=%s"%gps_latitude)
        if wifiinfoCnt != '0' :
            for i in range(0, int(wifiinfoCnt)) :

                offset =  wifinfo_end+1
                #print("offset=%d" % offset)
                apMACAddress = offset 
 
                apSignalStrength = offset + 1
                bandWidth = offset + 2
                rtt = offset + 3
                apSSID = offset + 4
                channel = offset + 5
                wifinfo_end = offset + 5
               # print('apMACAddress=%s'%rows[apMACAddress])
               # print('apSignalStrength=%s'%rows[apSignalStrength])
               # print('bandWidth=%s'%rows[bandWidth])
               # print('rtt=%s'%rows[rtt])
               # print('apSSID=%s'%rows[apSSID])
               # print('channel=%s'%rows[channel])

                wifiinfo.append({'apMACAddress':str(rows[apMACAddress]),'apSignalStrength':str(rows[apSignalStrength]), 'bandWidth':str(rows[bandWidth]),'rtt':str(rows[rtt]), 'apSSID':str(rows[apSSID]),'channel':str(rows[channel])})
        else :
                wifiinfo.append({'apMACAddress':'','apSignalStrength':'', 'bandWidth':'','rtt':'', 'apSSID':'','channel':''})
                #wifiinfo.append({})
        #print('apMACAddress%s' % wifiinfo[0]['apSignalStrength'])
        #print("ble start")
        #print("gt_latitude=%s" % gt_latitude)
        #print("ble_start=%d" % (wifinfo_end + 1))
        ble_start =   wifinfo_end + 1 
        btinfoCnt =  "0" if None else rows[ble_start]
        ble_end = ble_start 
        btinfo =[]
        #print("ble_cnt=%s" %btinfoCnt)

        if btinfoCnt != "0" :
            for i in range(0 , int(btinfoCnt)) :
         
                offset = ble_end + 1
                btMACAddress = offset 
                btSignalStrength= offset + 1
                btDeviceName = offset + 2
                ble_end = offset + 2
                btinfo.append({'btMACAddress':rows[btMACAddress],'btSignalStrength':rows[btSignalStrength], 'btDeviceName':rows[btDeviceName]})
        else :
                btinfo.append({'btMACAddress':'','btSignalStrength':'', 'btDeviceName':''})
                #btinfo.append({})
        #print("ble_end=%d" %ble_end)
        magneticData=[]  
        magnetic_start =  ble_end + 1
        magneticDataCnt = int(rows[magnetic_start])
        magnetic_end = magnetic_start
        #print("magneticDataCnt=%d" %magneticDataCnt)
        if magneticDataCnt != 0 :

            for i in range(0 , int(magneticDataCnt)) :
         
                offset = magnetic_end+1
                magX = offset 
                magY= offset + 1
                magZ = offset + 2
                magnetic_end = offset + 2
               # print("magnetic_end=%s" %rows[magnetic_end])
                magneticData.append({'magX':str(rows[magX]),'magY':str(rows[magY]), 'magZ':str(rows[magZ])})
        else :

                magneticData.append({'magX':'','magY':'', 'magZ':''})          
                #magneticData.append({})         


        cell_flag_offset = magnetic_end + 1

        cell_infoFlag = rows[cell_flag_offset]
        
        #print("cell_infoFlag=%s" % cell_infoFlag)


        """
        global cell_mcc 
        global cell_mnc 
        global cell_nettype 
        global cell_ci 
        global cell_beamid
        global cell_ta
        global cell_pci 
        global cell_tac 
        global cell_rsrp 
        global cell_rsrq
        global cell_nr_arfn_downlink 
        global cell_nr_earfcn_uplink
        global cell_band 
        global cell_bandwidth 
        global cell_rssi 
        global cell_tx_power 
        global cell_sinr 
        global cell_ri 
        global cell_rrc 
        global cell_ip
        global cell_cqi 
        global cell_ca 
        global cell_s_pci 
        global cell_s_freq 
        global cell_s_bandwidth 
        global cell_s_rsrp 
        global cell_s_rsrq 
        global cell_s_sinr 
        global cell_s_beamid 
        global cell_s_ta 
        global cell_s2_ca
        global cell_s2_pci
        global cell_s2_freq
        global cell_s2_bandwidth
        global cell_s2_rsrp
        global cell_s2_rsrq 
        global cell_s2_sinr 
        global cell_s2_beamid 
        global cell_s2_ta 
        global cell_s3_ca 
        global cell_s3_pci 
        global cell_s3_freq 
        global cell_s3_bandwidth 
        global cell_s3_rsrp 
        global cell_s3_rsrq 
        global cell_s3_sinr 
        global cell_s3_baemid 
        global cell_s3_ta 
        global cell_ref_info
        global cell_ref_info_start
        global cell_ref_info_end
        """    
        cell_ref_info=[]
        if cell_infoFlag == "1" :
            cell_start = cell_flag_offset 
            #print("cell_start=%s" % cell_start)

            cell_mcc = str(rows[(cell_start+1)])
            cell_mnc = str(rows[(cell_start+2)])
            cell_nettype =rows[(cell_start+3)]
            cell_ci = str(rows[(cell_start+4)])
            cell_beamid = str(rows[(cell_start+5)])
            cell_ta= rows[(cell_start+6)]
            cell_pci = str(rows[(cell_start+7)])
            cell_tac = str(rows[(cell_start+8)])
            cell_rsrp = str(rows[(cell_start+9)])
            cell_rsrq= str(rows[(cell_start+10)])
            cell_nr_arfn_downlink = str(rows[(cell_start+11)])
            cell_nr_earfcn_uplink=str(rows[(cell_start+12)])
            cell_band = str(rows[(cell_start+13)])
            cell_bandwidth = str(rows[(cell_start+14)])
            cell_rssi = str(rows[(cell_start+15)])
            cell_tx_power = str(rows[(cell_start+16)])
            cell_sinr = str(rows[(cell_start+17)])
            cell_ri = str(rows[(cell_start+18)])
            cell_rrc =str(rows[(cell_start+19)])
            cell_ip = str(rows[(cell_start+20)])
            cell_cqi = str(rows[(cell_start+21)])
            cell_ca = str(rows[(cell_start+22)])
            cell_s_pci = str(rows[(cell_start+23)])
            cell_s_freq = str(rows[(cell_start+24)])
            cell_s_bandwidth = str(rows[(cell_start+25)])
            cell_s_rsrp = str(rows[(cell_start+26)])
            cell_s_rsrq = str(rows[(cell_start+27)])
            cell_s_sinr = str(rows[(cell_start+28)])
            cell_s_beamid = str(rows[(cell_start+29)])
            cell_s_ta = str(rows[(cell_start+30)])
            cell_s2_ca = str(rows[(cell_start+31)])
            cell_s2_pci= str(rows[(cell_start+32)])
            cell_s2_freq= str(rows[(cell_start+33)])
            cell_s2_bandwidth=str(rows[(cell_start+34)])
            cell_s2_rsrp = str(rows[(cell_start+35)])
            cell_s2_rsrq = str(rows[(cell_start+36)])
            cell_s2_sinr = str(rows[(cell_start+37)])
            cell_s2_beamid = str(rows[(cell_start+38)])
            cell_s2_ta = str(rows[(cell_start+39)])
            cell_s3_ca = str(rows[(cell_start+40)])
            cell_s3_pci = str(rows[(cell_start+41)])
            cell_s3_freq = str(rows[(cell_start+42)])
            cell_s3_bandwidth = str(rows[(cell_start+43)])
            cell_s3_rsrp = str(rows[(cell_start+44)])
            cell_s3_rsrq = str(rows[(cell_start+45)])
            cell_s3_sinr = str(rows[(cell_start+46)])
            cell_s3_baemid = str(rows[(cell_start+47)])
            cell_s3_ta = str(rows[(cell_start+48)])

            cell_MrleCnt = rows[(cell_start+49)]

            #print("cell_mcc=%s" % (cell_mcc))
            #print("cell_mnc=%s" % (cell_mnc))
            #print("cell_Mrleindex=%s" % (cell_start+49))
            #print("cell_Mrlecnt=%s" % str(cell_MrleCnt))
            #print("cell_beamid=%s" % str(cell_beamid))
            cell_ref_info_end = cell_start+49

            if cell_MrleCnt != "0" :
    
                cell_ref_info_start = (cell_start+49) + 1
                cell_ref_info_end=cell_ref_info_start

                for i in range(0, int(cell_MrleCnt)) :
                
                    offset =  cell_ref_info_end
                   # print("cell=%d" % offset)

                    beamid = offset 
                    pci = offset + 1
                    rsrp = offset + 2

                    rsrq = offset + 3
                    freq = offset + 4
                    sinr = offset + 5
                    cell_ref_info_end = sinr + 1
                    #print(cell_ref_info)
                    #print("sinr=%s"%rows[sinr])
                    cell_ref_info.append({'beamid':str(rows[beamid]), 'pci':str(rows[pci]), 'rsrp':str(rows[rsrp]),'rsrq':str(rows[rsrq]),'freq':str(rows[freq]),'sinr':str(rows[sinr])})
            else :
                cell_ref_info.append({'beamid':'', 'pci':'', 'rsrp':'','rsrq': '','freq':'', 'sinr':''})  
                #cell_ref_info.append({})  
                

            cell_flag2_offset = cell_ref_info_end 
            #print("cell_flag2_offset=%d" % cell_flag2_offset)
            #print("cell_flag2_offset=%s" % rows[cell_flag2_offset])
            cell_infoFlag2 =  rows[cell_flag2_offset] 
        
        else :
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
            #cell_ref_info.append({})  
        """
        global cell2_mcc 
        global cell2_mnc 
        global cell2_nettype 
        global cell2_ci 
        global cell2_beamid 
        global cell2_ta
        global cell2_pci 
        global cell2_tac 
        global cell2_rsrp 
        global cell2_rsrq
        global cell2_nr_arfn_downlink 
        global cell2_nr_earfcn_uplink
        global cell2_band 
        global cell2_bandwidth
        global cell2_rssi
        global cell2_tx_power
        global cell2_sinr
        global cell2_ri 
        global cell2_rrc 
        global cell2_ip 
        global cell2_cqi 
        global cell2_ca 
        global cell2_s_pci 
        global cell2_s_freq 
        global cell2_s_bandwidth 
        global cell2_s_rsrp 
        global cell2_s_rsrq 
        global cell2_s_sinr 
        global cell2_s_beamid 
        global cell2_s_ta 
        global cell2_s2_ca 
        global cell2_s2_pci
        global cell2_s2_freq
        global cell2_s2_bandwidth
        global cell2_s2_rsrp 
        global cell2_s2_rsrq
        global cell2_s2_sinr 
        global cell2_s2_beamid 
        global cell2_s2_ta 
        global cell2_s3_ca 
        global cell2_s3_pci 
        global cell2_s3_freq 
        global cell2_s3_bandwidth 
        global cell2_s3_rsrp 
        global cell2_s3_rsrq 
        global cell2_s3_sinr 
        global cell2_s3_baemid 
        global cell2_s3_ta 
        global cell2_ref_info
        global cell2_ref_info_start 
        global cell2_ref_info_end

        if cell_infoFlag2 == 1 :
            cell_start = cell_flag2_offset + 1 

            cell2_mcc = rows[(cell_start+1)]
            cell2_mnc = rows[(cell_start+2)]
            cell2_nettype =rows[(cell_start+3)]
            cell2_ci = rows[(cell_start+4)]
            cell2_beamid = rows[(cell_start+5)]
            cell2_ta= rows[(cell_start+6)]
            cell2_pci = rows[(cell_start+7)]
            cell2_tac = rows[(cell_start+8)]
            cell2_rsrp = rows[(cell_start+9)]
            cell2_rsrq= rows[(cell_start+10)]
            cell2_nr_arfn_downlink = rows[(cell_start+11)]
            cell2_nr_earfcn_uplink=rows[(cell_start+12)]
            cell2_band = rows[(cell_start+13)]
            cell2_bandwidth = rows[(cell_start+14)]
            cell2_rssi = rows[(cell_start+15)]
            cell2_tx_power = rows[(cell_start+16)]
            cell2_sinr = rows[(cell_start+17)]
            cell2_ri = rows[(cell_start+18)]
            cell2_rrc =rows[(cell_start+19)]
            cell2_ip = rows[(cell_start+20)]
            cell2_cqi = rows[(cell_start+21)]
            cell2_ca = rows[(cell_start+22)]
            cell2_s_pci = rows[(cell_start+23)]
            cell2_s_freq = rows[(cell_start+24)]
            cell2_s_bandwidth = rows[(cell_start+25)]
            cell2_s_rsrp = rows[(cell_start+26)]
            cell2_s_rsrq = rows[(cell_start+27)]
            cell2_s_sinr = rows[(cell_start+28)]
            cell2_s_beamid = rows[(cell_start+29)]
            cell2_s_ta = rows[(cell_start+30)]
            cell2_s2_ca = rows[(cell_start+31)]
            cell2_s2_pci= rows[(cell_start+32)]
            cell2_s2_freq= rows[(cell_start+33)]
            cell2_s2_bandwidth=rows[(cell_start+34)]
            cell2_s2_rsrp = rows[(cell_start+35)]
            cell2_s2_rsrq = rows[(cell_start+36)]
            cell2_s2_sinr = rows[(cell_start+37)]
            cell2_s2_beamid = rows[(cell_start+38)]
            cell2_s2_ta = rows[(cell_start+39)]
            cell2_s3_ca = rows[(cell_start+40)]
            cell2_s3_pci = rows[(cell_start+41)]
            cell2_s3_freq = rows[(cell_start+42)]
            cell2_s3_bandwidth = rows[(cell_start+43)]
            cell2_s3_rsrp = rows[(cell_start+44)]
            cell2_s3_rsrq = rows[(cell_start+45)]
            cell2_s3_sinr = rows[(cell_start+46)]
            cell2_s3_baemid = rows[(cell_start+47)]
            cell2_s3_ta = rows[(cell_start+48)]

            cell2_MrleCnt = rows[(cell_start+49)]
        
            cell2_ref_info=[]
            cell2_ref_info_start = (cell_start+49) + 1
            cell2_ref_info_end=cell2_ref_info_start

            for i in range(0, int(cell2_MrleCnt)) :

                offset =  cell2_ref_info_end+1
                print("offset=%d" % offset)

                beamid = offset 
                pci = offset + 1
                rsrp = offset + 2
                rsrq = offset + 3
                freq = offset + 4
                sinr = offset + 5
                cell2_ref_info_end = offset + 5

                cell2_ref_info.append([{'beamid':rows[beamid], 'pci':rows[pci], 'rsrp':rows[rsrp],'rsrq':rows[rsrq],'freq':rows[freq],'sinr':rows[sinr]}])
        """
        pd.set_option('display.max_columns', None)
        time_2 = datetime.datetime.fromtimestamp(int(time_1))
        
        collect_dt = time_2.strftime('%Y-%m-%d')
        

        record = {}
        record['collect_dt'] = collect_dt
        record['idcsstoretime']=time_1
        record['android_id'] = android_id
        record['protocolversion'] = protocol_version
        record['hpsclientversion'] = hpsclient_version
        record['msmodel'] = msmodel
        record['servuc_id'] = servuc_id
        record['mcc'] = mcc
        record['nettype'] = nettype
        record['appid'] = appid
        record['collecttype']=type
        record['groupid'] = group_id
        record['in_out_none'] = in_out
        record['provider'] = provider
        record['collecttime'] = time_stamp
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
        record['btinfocnt'] = btinfoCnt
        record['bt_info'] = btinfo
        record['magneticDataCnt'] = magneticDataCnt
        record['magnetic_info'] = magneticData

        record['cell_infoFlag']=cell_infoFlag

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
       
        #print(record)
        #records = []
        #records.append()

        """
        row_apped = pd.DataFrame( {'dt': date, 'time':time_1  , 'android_id':android_id , 'protocol_version': protocol_version , 
                                  'hpsclient_version':hpsclient_version ,  'msmodel':msmodel , 'serucid': serucid, 'mcc': mcc, 'nettype':nettype,
                                  'appid':appid , 'type':str(type), 'group_id':group_id, 'in_out':in_out, 'provider':provider, 'time_stamp':time_stamp ,
                                  'gt_latitude':  gt_latitude ,'gt_longitude':gt_longitude, 'gt_accuracy':gt_accuracy, 'gt_building':gt_building ,
                                  'gt_floor': gt_floor, 'gt_poi':gt_poi,  'gps_latitude':gps_latitude, 'gps_longitude': gps_longitude,
                                  'gps_accuracy':gps_accuracy , 'gps_velocity': gps_velocity , 'gps_timestamp': gps_timestamp ,
                                  'gps_dop': str(gps_dop) , 'gps_hepe': str(gps_hepe) , 'numpgs': str(numgps) ,
                                  'fused_lattitude' : fused_latitude , 'fused_longitude': fused_longitude , 'fused_accuracy': str(fused_accuracy),
                                  'fused_timestamp': fused_timestamp , 'hps_latitude' : hps_latitude , 'hps_longitude' : hps_longitude ,
                                  'hps_accuracy': str(hps_accuracy) , 'hps_timestamp': hps_timestamp , 'hps_fixtype': hps_fixtype,
                                  'hps_building': hps_building , 'hps_floor': str(hps_floor) , 'hps_poi': hps_poi, 'airpress': str(airpress),
                                  'detectedactivity': str(detectedactivity),
                                  'wifi_ap_cnt':wifiinfoCnt, 'wifi_ap': wifiinfo , 'btinfoCnt':btinfoCnt, 'bt_info':   btinfo, 'magneticDataCnt':magneticDataCnt , 'magnetic_info':magneticData ,
                                  'cell_infoFlag':str(cell_infoFlag), 'cell_mcc':cell_mcc, 'cell_mnc':cell_mnc ,'cell_nettype':cell_nettype,
                                  'cell_ci':cell_ci , 'cell_beamid':cell_beamid,'cell_ta':cell_ta, 'cell_pci':cell_pci, 'cell_tac':cell_tac,
                                  'cell_rsrp':cell_rsrp, 'cell_rsrq':cell_rsrq, 'cell_nr_arfn_downlink':cell_nr_arfn_downlink, 'cell_nr_earfcn_uplink':cell_nr_earfcn_uplink, 'cell_band':cell_band, 'cell_bandwidth':cell_bandwidth,
                                  'cell_rssi':cell_rssi, 'cell_tx_power':cell_tx_power, 'cell_sinr':cell_sinr, 'cell_ri':cell_ri, 'cell_rrc':cell_rrc,
                                  'cell_ip':cell_ip, 'cell_cqi':cell_cqi, 'cell_ca':cell_ca, 'cell_s_pci':cell_s_pci , 'cell_s_freq':cell_s_freq,
                                  'cell_s_bandwidth':cell_s_bandwidth, 'cell_s_rsrp':cell_s_rsrp ,'cell_s_rsrq':cell_s_rsrq ,  'cell_s_sinr': cell_s_sinr,
                                  'cell_s_beamid':cell_s_beamid, 'cell_s_ta':cell_s_ta,  'cell_s2_ca':cell_s2_ca ,'cell_s2_pci': cell_s2_pci,
                                  'cell_s2_freq':cell_s2_freq , 'cell_s2_bandwidth':cell_s2_bandwidth, 'cell_s2_rsrp':cell_s2_rsrp ,'cell_s2_rsrq':cell_s2_rsrq,
                                  'cell_s2_sinr':cell_s2_sinr ,'cell_s2_beamid': cell_s2_beamid, 'cell_s2_ta': cell_s2_ta, 'cell_s3_ca': cell_s3_ca, 
                                  'cell_s3_pci':cell_s3_pci ,'cell_s3_freq':cell_s3_freq , 'cell_s3_bandwidth':cell_s3_bandwidth ,'cell_s3_rsrp':cell_s3_rsrp,  
                                  'cell_s3_rsrq':cell_s3_rsrq ,  'cell_s3_sinr':cell_s3_sinr, 'cell_s3_baemid':cell_s3_baemid, 'cell_s3_ta':cell_s3_ta ,'cell_MrleCnt':str(cell_MrleCnt) , 'cell_ref_info': str(cell_ref_info )})
        """
        
        return record
                         

def read_hpsclient(csv_file) :

  #csv_file="/Users/1111764/Documents/location/development/hps_client/sample_data/원본 RAW_DATA/202304/context_in_type_2_2023040400.csv"
    #options = csv.ReadOptions(encoding='utf-8',delimiter="|") 
    #csv_file = open(csv_file, "r", encoding="utf8", errors="", newline="" )
    #f= csv.reader(csv_file, delimiter='|')
    #f=parse_options = csv.ParseOptions(delimseiter="|", invalid_row_handler=skip_comment)
    #f =csv.read_csv(csv_file,parse_options=csv.ParseOptions(delimiter=','),read_options=csv.ReadOptions(encoding='utf8'))
    #f =pd.read_csv(csv_file)  

    #f = pd.read_csv(csv_file,  parse_options=csv.ParseOptions(delimiter="|"))
  

    #f = pd.read_csv(csv_file , engine="python",header=None, sep="|",encoding = "utf-8", quotechar='"' ,on_bad_lines=lambda x: x[:-1])
    #f = pd.read_csv(csv_file , engine="python",header=None, sep="|",encoding = "utf-8" ,on_bad_lines='skip')
    #f = pd.read_csv(csv_file , engine="python",header=None, sep="|",encoding = "utf-8" ,on_bad_lines=partial(write_bad_line, sep='|', fp=bad_lines_fp),encoding_errors="replace")
    #csv_input = pd.read_csv(filepath_or_buffer=csv_file, encoding="utf8", sep="|")

  #df = pd.DataFrame()
  with open(csv_file, 'r', encoding='utf-8', errors='replace') as file:
       
    lines = file.readlines()
    print("** csv_file:", csv_file)
    #df = pd.DataFrame()
    list_row = []

    for i, line in enumerate(lines):
        #print("** index name:", i)
        #print("** csv_file:", csv_file)
        rows= line.strip().split('|') 
               
        cleaned_row = [remove_non_ascii(str(cell)) for cell in rows]
       
        
        try: 
            if check_wifi_data_type(cleaned_row):
              #  print("test")
                #print (makeRow(cleaned_row))
                #df = pd.concat([df,makeRow(cleaned_row)])
                #df.set_index()
                list_row.append(makeRow(cleaned_row))
        except Exception as e:
            #print(cleaned_row)
            print(e)
            print(f"{str(i)} Parse fail ")
            pass
    #df = pd.DataFrame(list_row,parquet_schema_new)
    df = pd.DataFrame(list_row)
    #print(df)

    #pprint.pprint(df.describe, width=1000, indent=4)

    #table =Table.from_pandas(df)   

    #print(table)
    table = Table.from_pandas(df, schema=parquet_schema_new)   
   # table = Table.from_pandas(df)  
    #df.to_parquet("/Users/1111764/Documents/location/development/hps_client/target/test.parquet", partition_cols=['dt', 'type'])
    #pq.write_to_dataset(table ,"/disk2/datadown/HPS_DATA/HPS_20230425/HPS.parquet",compression='GZIP',partition_cols=['collect_dt','collecttype'],use_dictionary=True)
    pq.write_to_dataset(table ,"/Users/1111764/Documents/location/development/TIDC/TIDC/hps_client/parsing/HPS.parquet",compression='GZIP',partition_cols=['collect_dt','collecttype'],use_dictionary=True)
 
    # df.to_orc(path="/Users/1111764/Documents/location/development/hps_client/target/test.orc",engine='pyarrow',index=True)
    #orc.write_table(table, "/Users/1111764/Documents/location/development/hps_client/target/test.orc")

    #df.to_csv("/Users/1111764/Documents/location/development/hps_client/target/test.csv", sep='^', encoding='utf-8')

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

if __name__ == "__main__":

    #dir_path = '/DATA/pms_app/PMS_DATA'
   
    #dir_path= os.path.join(dir_path, sys.argv[1])
    #dir_path= os.path.join(dir_path, "/WIFI_RAWDATA/LOG")
    
    yesterday= '20231120'
    dir_path="/Users/1111764/Documents/location/development/데이터작업"
    print (dir_path)
    print("%s 시작" % sys.argv[1])

    start_time = time.time()
    #read_hpsclient("/Users/1111764/Documents/location/development/데이터작업/20231021/WIFI_RAWDATA/LOG/38.CONTEXT_IN/context_in_type_5_2023102122.csv")
# list to store files
# Iterate directory

    for  (path, dir, files) in  os.walk(dir_path):
       #print(path)
       #print(dir)
    # check if current path is a file
       for dir_nm in dir :
          # print(dir_nm)
           if dir_nm in ("37.CONTEXT_OUT" , "38.CONTEXT_IN", "39.CONTEXT_NONE") :
                #if os.path.isfile(os.path.join(path, files)):
                    print(os.path.join(path, dir_nm))
                    dir_path = os.path.join(path, dir_nm)
                    #print(files)
                    dir_list = os.listdir(os.path.join(path, dir_nm))
                    for f_name in dir_list :
                        ext = f_name.split("_")[0]
                        if ext == 'context': 
                          file_name =  os.path.join(dir_path, f_name)
                          if os.stat(file_name).st_size > 0:
                            print(f_name)
                            read_hpsclient(file_name)
                           
    print("%s 종료 (%.2f 초) " % (sys.argv[1], time.time() - start_time))       


