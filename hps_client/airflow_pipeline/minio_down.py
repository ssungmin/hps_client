from minio import Minio
from minio.error import ResponseError

client = Minio(
    endpoint='172.27.11.59:9000',
    secure=False,
    access_key='hpe_client_key',
    secret_key='qwer4321!'
  )


try:

  for item in client.list_objects("tmap",recursive=True):
        client.fget_object("tmap",item.object_name,item.object_name)
        
except ResponseError as err:
  print(err)