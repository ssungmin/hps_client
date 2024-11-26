

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.dummy_operator import DummyOperator
import time
import os
from airflow.utils.dates import days_ago

import pendulum
import datetime
from datetime import timedelta

local_tz = pendulum.timezone("Asia/Seoul")

now =datetime.datetime.now() - timedelta(2)   
yesterday = now.strftime('%Y%m%d')


ssh_hook = SSHHook (
       ssh_conn_id ="spark" ,
       cmd_timeout=None , 
       username ="svcapp_su" ,
       remote_host="172.27.11.56" ,
       key_file=""
)

args = {'owner': 'sungmin',
        'start_date': datetime.datetime(2024,5, 31, 2, tzinfo=local_tz) }

dag = DAG(dag_id='spark',
          default_args=args,
          schedule_interval='0 06 * * *')


start_task  = DummyOperator(  task_id= "start" , dag=dag)
stop_task   = DummyOperator(  task_id= "stop" , dag=dag )


remote_file_path1 ="/home/svcapp_su/datapipeline/union_data.py"
run_file_command1 = f"python {remote_file_path1}"

t0 = SSHOperator( task_id='union_data',
                  ssh_hook=ssh_hook ,
                  command=run_file_command1 ,
                  dag=dag)



remote_file_path1 ="/home/svcapp_su/datapipeline/gt_filtering_data.py"
run_file_command1 = f"python {remote_file_path1}"

t1 = SSHOperator( task_id='gt_filtering',
                  ssh_hook=ssh_hook ,
                  command=run_file_command1 ,
                  dag=dag)

remote_file_path2 ="/home/svcapp_su/datapipeline/explode_ap.py"
run_file_command2 = f"python {remote_file_path2}"

t2= SSHOperator( task_id='explode_ap',
                  ssh_hook=ssh_hook ,
                  command=run_file_command2 ,
                  dag=dag)

remote_file_path3 ="/home/svcapp_su/datapipeline/movable_ap_filtering.py"
run_file_command3 = f"python {remote_file_path3}"

t3= SSHOperator( task_id='movable_ap',
                  ssh_hook=ssh_hook ,
                  command=run_file_command3 ,
                  dag=dag)

remote_file_path4 ="/home/svcapp_su/datapipeline/dnn_feature_save.py"
run_file_command4 = f"python {remote_file_path4}"

t4= SSHOperator( task_id='dnn_feature',
                  ssh_hook=ssh_hook ,
                  command=run_file_command4 ,
                  dag=dag)

start_task >> t0 >> t1 >> t2 >> t3 >> t4 >> stop_task