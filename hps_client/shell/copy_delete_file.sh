
#! /bin/bash


1. 할일 D-1 일 날짜로 data를 /DATA폴더로 전달 : 오후 2시?
2. HOME 폴더에서 해당일 데이터 삭제
3. /DATA 폴더에는 1주일 치의 데이터만 남기고 삭제 

param1=$1  # a
yesterday=`date +%Y%m%d -d '-1days'`

find . -type f -name "*20231010*" -exec rm {} \; 
find . -type f -name "*20231011*" -exec rm {} \; 
find . -type f -name "*20231012*" -exec rm {} \; 
find . -type f -name "*20231013*" -exec rm {} \; 
find . -type f -name "*20231014*" -exec rm {} \; 
find . -type f -name "*20231015*" -exec rm {} \; 

find . -type f -name "*20231001*" -exec rm {} \;
find . -type f -name "*20231002*" -exec rm {} \;
find . -type f -name "*20231003*" -exec rm {} \;

find . -type f -name "*20230914*" -exec rm {} \; 
find . -type f -name "*20230915*" -exec rm {} \; 
find . -type f -name "*20230916*" -exec rm {} \; 
find . -type f -name "*20230917*" -exec rm {} \; 

find . -name "*20231010*" -exec cp -f {} --parents /DATA/pms_app/PMS_DATA/20231010 \;
find . -name "*20231011*" -exec cp -f {} --parents /DATA/pms_app/PMS_DATA/20231011 \;
find . -name "*20231012*" -exec cp -f {} --parents /DATA/pms_app/PMS_DATA/20231012 \;
find . -name "*20231013*" -exec cp -f {} --parents /DATA/pms_app/PMS_DATA/20231013 \;
find . -name "*20231014*" -exec cp -f {} --parents /DATA/pms_app/PMS_DATA/20231014 \;
find . -name "*20231015*" -exec cp -f {} --parents /DATA/pms_app/PMS_DATA/20231015 \;

find . -name "*20231001*" -exec cp -f {} --parents /DATA/pms_app/PMS_DATA/20231001 \;

find . -name "*20231002*" -exec cp -f {} --parents /DATA/pms_app/PMS_DATA/20231002 \;
find . -name "*20231003*" -exec cp -f {} --parents /DATA/pms_app/PMS_DATA/20231003 \;


find . -name "*20230914*" -exec cp -f {} --parents /DATA/pms_app/PMS_DATA/20230914 \;
find . -name "*20230915*" -exec cp -f {} --parents /DATA/pms_app/PMS_DATA/20230915 \;
find . -name "*20230916*" -exec cp -f {} --parents /DATA/pms_app/PMS_DATA/20230916 \;
find . -name "*20230917*" -exec cp -f {} --parents /DATA/pms_app/PMS_DATA/20230917 \;

du -h . -d 3



find . -type f -name "context*2023092*" -exec tar -rvf context.tar {} +;

tde2-SMU8SuHpbuePiUvBfVVi



sftp -o "ProxyCommand connect -H https://tidcex.sktelecom.com %h %p" svcapp@172.27.12.112:10021