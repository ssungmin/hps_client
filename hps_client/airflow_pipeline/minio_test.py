#!/usr/bin/env/python
from minio import Minio
from minio.error import S3Error
import pandas as pd
import os
import glob
from datetime import datetime
from datetime import date, timedelta

#!/bin/python
from pyarrow import fs, csv, parquet
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import Table

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

def upload_local_directory_to_minio(local_path: str, bucket_name: str, minio_path):
    assert os.path.isdir(local_path)

    for local_file in glob.glob(local_path + '/**'):
        #local_file = local_file.replace(os.sep, "/")
        if not os.path.isfile(local_file):
            upload_local_directory_to_minio(
                local_file, bucket_name,minio_path + "/" + os.path.basename(local_file))
        else:
            remote_path = os.path.join(minio_path , local_file[1+len(local_path):])
            client.fput_object(bucket_name, remote_path, local_file)


def main():
  # Create a client with the MinIO server playground, its access key
  # and secret key.

  #df = pd.read_parquet('/home/svcapp_su/source/data/HPS_CLEINT.parquet',engine='pyarrow')  
  # Make 'asiatrip' bucket if not exist.
  #found = client.bucket_exists("test")
  #if not found:
  #  client.make_bucket("test")
  #else:
  #  print("Bucket 'test' already exists")

  #now =datetime.now() - timedelta(1)  
  #today_datetime = now.strftime('%Y%m%d')
  upload_local_directory_to_minio ("/DATA/pms_app/PMS_DATA/20231111","raw-data","20231111")
  

  ## read 
  #s3_filepath = "hps-client/HPS.parquet/"

  #pf = pq.ParquetDataset(
  #     s3_filepath,
  #     filesystem=minio,
  #     use_legacy_dataset=False)
  #df = pf.read().to_pandas()

  #print(df)

if __name__ == "__main__":
  try:
    main()
  except S3Error as exc:
    print(exc)