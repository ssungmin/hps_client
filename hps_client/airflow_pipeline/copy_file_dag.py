

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
import time
import os
from airflow.utils.dates import days_ago
import copy_delete_file 
import parser_hps_client_object
import parser_tmap_object
import pendulum
import datetime
from datetime import timedelta
from airflow.utils.task_group import TaskGroup


local_tz = pendulum.timezone("Asia/Seoul")

now =datetime.datetime.now() - timedelta(1)   
yesterday = now.strftime('%Y%m%d')

args = {'owner': 'sungmin',
        'start_date': datetime.datetime(2023, 10, 29, 2, tzinfo=local_tz) }



with DAG(dag_id="hps_client_file_backup", default_args=args, schedule_interval='0 14 * * *') as dag:

    # A dummy task to start the DAG
    start_task  = DummyOperator(  task_id= "start" , dag=dag)
    stop_task   = DummyOperator(  task_id= "stop" , dag=dag )

    t1 = PythonOperator(task_id='backup_file',
                    provide_context=True,
                    python_callable=copy_delete_file.main,
                    #op_kwargs={'key1': 'value1', 'key2': 'value2'},
                    #op_args=['one', 'two', 'three'],
                    dag=dag)
    # A task group to process the data
    with TaskGroup(group_id="process_data") as process_data:

        # A task to read the data from a CSV file
        t2 = PythonOperator(task_id='parquet_parsing_hps_client',
                    provide_context=True,
                    python_callable=parser_hps_client_object.main,
                    #op_kwargs={'key1': 'value1', 'key2': 'value2'},
                    op_args=[yesterday],
                    dag=dag)

        t3= PythonOperator(task_id='parquet_parsing_tmap',
                    provide_context=True,
                    python_callable=parser_tmap_object.main,
                    #op_kwargs={'key1': 'value1', 'key2': 'value2'},
                    op_args=[yesterday],
                    dag=dag)

    # A dummy task to end the DAG


    # Define the dependencies
    start_task >> t1 >> process_data >> stop_task