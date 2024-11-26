import pandas as pd
import pyarrow as pa

import pyarrow.parquet as pq
from minio import Minio
from minio.error import S3Error
from pyarrow import fs, csv, parquet
import ray
from ray.util.dask import enable_dask_on_ray
#import raydp
import dask
ray.init(address="ray://10.1.1.132:10001")

minio = fs.S3FileSystem(
endpoint_override='10.1.1.132:9000',
access_key='minioadmin',
secret_key='minioadmin',
scheme='http')

df =  ray.data.read_parquet(filesystem=minio , paths="hps-client/HPS.parquet")
#print(df.take(1))

df2 = df.to_dask()
df2.head()