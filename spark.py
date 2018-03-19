#
#
# from __future__ import print_function
# from airflow.operators.python_operator import PythonOperator
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators.email_operator import EmailOperator
# from airflow.hooks.presto_hook import PrestoHook
# from airflow.operators.bash_operator import BashOperator
# from sqlalchemy.sql import text as sa_text
# from collections import deque
#
# from airflow.hooks.hive_hooks import HiveCliHook
# from airflow.hooks.hive_hooks import HiveServer2Hook
# from airflow.operators.hive_operator import HiveOperator
# import os
# import glob
# from airflow.hooks.mysql_hook import MySqlHook
# from airflow.models import DAG
# import time
# import pandas as pd
# import pickle
# from datetime import datetime, timedelta
# import os
# import boto3
# import glob
# from src.sell_amt_utils import *
#
#
#
# def send_data_to_s3(**param):
#     partition_end = param['item_part']
#     file = os.path.abspath(os.path.join(os.path.dirname(__file__), "../tmp_feathers/raw/raw_%s.feather" % str(partition_end)))
#
#     # files_to_transfer = glob.glob(path)
#     s3 = boto3.resource('s3')
#
#     # for file in files_to_transfer:
#     data = open(file, 'rb')
#     filename = "raw/"+file.split('/')[-1]
#     print("sending...." + filename)
#     s3.Bucket('epopcon-ds').put_object(Key=filename, Body=data)
#
#     # print('sending data...')
#     # for idx, _ in enumerate(range(10)):
#     #     data = open('/home/ws-cps/airflow/airflow_home/tmp_feathers/processed/processed_149.feather', 'rb')
#     #     print(idx)
#     #     s3.Bucket('epopcon-ds').put_object(Key='processed_149.feather_%s' % idx, Body=data)
#
# def retrieve_dataset_from_presto(**param):
#
#
#     partition_start, partition_end = param['item_part']
#
#     if DEVELOPMENT:
#         result = presto.get_pandas_df(
#             hql="SELECT ID, ITEM_ID, STOCK_ID, STOCK_AMOUNT, COLLECT_DAY, REG_ID, REG_DT FROM inventory_part WHERE item_part BETWEEN %s AND %s AND month_part >= 3 LIMIT 500" % (partition_start, partition_end))
#     else:
#         result = presto.get_pandas_df(hql="SELECT ID, ITEM_ID, STOCK_ID, STOCK_AMOUNT, COLLECT_DAY, REG_ID, REG_DT FROM inventory_part WHERE item_part BETWEEN %s AND %s AND month_part >= 3" % (partition_start, partition_end))
#
#     result.columns = ["ID", "ITEM_ID", "STOCK_ID", "STOCK_AMOUNT", "COLLECT_DAY", "REG_ID", "REG_DT"]
#     result.REG_DT = pd.to_datetime(result.REG_DT)
#
#     logger.info("time to retrieve partition %s from server...." % partition_end)
#     write_to_feather(param['item_part'], result, processed=False)
#     logger.info("retrieving partition %s done" % partition_end)
#
# default_args = {
#     'owner': 'Daniel',
#     'depends_on_past': False,
#     'start_date': datetime(2018, 3, 11),
#     'email_on_failure': True,
#     'email_on_retry': True,
#     'retries': 5,
#     'retry_delay': timedelta(minutes=1),
# }
#
# presto = PrestoHook()
# DEVELOPMENT = False
# EMAIL_LIST = [
#                 'daniel.kim@epopcon.com'
#                 # ,'zururux@epopcon.com'
# ]
#
# dag = DAG('transfer_to_s3_2',
#           default_args=default_args,
#           dagrun_timeout=timedelta(10),
#           description='Modeling the sell amount',
#           schedule_interval='0 0 * * 0',
#           # schedule_interval="@once",
#           catchup=False)
# begin_task = DummyOperator(task_id='begin_task', dag=dag)
#
# success_email_task = EmailOperator(
#                 task_id='success_email_task',
#                 to=EMAIL_LIST,
#                 subject='success_email_task',
#                 html_content='success_email_task', dag=dag)
#
#
# np_array = np.array(range(0, 855))
# list_of_arrays = np.array_split(np_array, 400)
#
# range_list = []
# for array in list_of_arrays:
#     range_list.append((array[0], array[-1]))
#
#
# for range_idxs in range_list:
#
#     retrieve_dataset_from_presto_task = PythonOperator(
#                                 task_id='retrieve_dataset_from_presto_task_%s' % range_idxs[1],
#                                 op_kwargs={'item_part': range_idxs},
#                                 python_callable=retrieve_dataset_from_presto,
#                                 priority_weight=1,
#                                 dag=dag)
#
#     send_data_task = PythonOperator(
#                                 task_id='send_data_task_%s' % range_idxs[1],
#                                 python_callable=send_data_to_s3,
#                                 priority_weight=10,
#                                 op_kwargs={'item_part': range_idxs[1]},
#                                 dag=dag)
#
#     begin_task >> retrieve_dataset_from_presto_task >> send_data_task >> success_email_task