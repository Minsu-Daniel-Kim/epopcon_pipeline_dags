
from __future__ import print_function
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.hooks.presto_hook import PrestoHook
from airflow.operators.bash_operator import BashOperator
from sqlalchemy.sql import text as sa_text
from collections import deque

from airflow.hooks.hive_hooks import HiveCliHook
from airflow.hooks.hive_hooks import HiveServer2Hook
from airflow.operators.hive_operator import HiveOperator
import os
import glob
from airflow.hooks.mysql_hook import MySqlHook
from airflow.models import DAG
import time
import pandas as pd
import pickle
from datetime import datetime, timedelta
import os
from src.sell_amt_utils import *

import logging

logger = logging.getLogger()

default_args = {
    'owner': 'Daniel',
    'depends_on_past': False,
    'start_date': datetime(2018, 3, 11),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
}





def retrieve_dataset_from_presto(**param):


    partition = param['item_part']

    if DEVELOPMENT:
        result = presto.get_pandas_df(
            hql="SELECT ID, ITEM_ID, STOCK_ID, STOCK_AMOUNT, COLLECT_DAY, REG_ID, REG_DT FROM inventory_part WHERE item_part = %s AND month_part >= 3 LIMIT 500" % partition)
    else:
        result = presto.get_pandas_df(hql="SELECT ID, ITEM_ID, STOCK_ID, STOCK_AMOUNT, COLLECT_DAY, REG_ID, REG_DT FROM inventory_part WHERE item_part = %s AND month_part >= 3" % partition)

    result.columns = ["ID", "ITEM_ID", "STOCK_ID", "STOCK_AMOUNT", "COLLECT_DAY", "REG_ID", "REG_DT"]
    result.REG_DT = pd.to_datetime(result.REG_DT)

    logger.info("time to retrieve partition %s from server...." % partition)
    write_to_feather(partition, result, processed=False)
    logger.info("retrieving partition %s done" % partition)


def truncate_sell_amt_table_op():
    wspider_temp_engine.execute(
        sa_text('''TRUNCATE TABLE wspider_temp.MWS_COLT_ITEM_SELL_AMT_DEV''').execution_options(autocommit=True))

def apply_model_op(**param):
    partition = param['item_part']
    dataset = read_feather(partition, processed=False)
    apply_model(partition, dataset)

def transfer_to_mysql_op(**param):
    partition = param['item_part']
    engine = param['engine']
    dataset = read_feather(partition, processed=True)
    wspider_temp_engine.dispose()
    dataset.to_sql(name='MWS_COLT_ITEM_SELL_AMT_DEV', con=engine, if_exists='append', index=False, chunksize=10000)

# def insert_sell_amt_op():
#     result = presto.get_pandas_df(hql="SELECT * FROM inventory_part WHERE item_part = '820' LIMIT 100")
#     # print(result.columns)
#
#     result.columns = [col.upper()[1:] for col in result.columns]
#     print(result.columns)
#
#     insert_sell_amt(result)

presto = PrestoHook()
DEVELOPMENT = False
EMAIL_LIST = [
                'daniel.kim@epopcon.com',
                'zururux@epopcon.com'
]

dag = DAG('sell_amt_modeling_dag_production_weekly',
          default_args=default_args,
          dagrun_timeout=timedelta(10),
          description='Modeling the sell amount',
          schedule_interval='0 0 * * 0',
          # schedule_interval="@once",
          catchup=False)

# begin_task = DummyOperator(task_id='begin_task', dag=dag)

truncate_sell_amt_table_task = PythonOperator(
                            task_id='truncate_sell_amt_table_task',
                            python_callable=truncate_sell_amt_table_op,
                            dag=dag)

# retrieving_email_task = EmailOperator(
#                 task_id='retrieving_email_task',
#                 to=EMAIL_LIST,
#                 subject='Retrieving part is successfully done! [1/3]',
#                 html_content='Retrieving part => Modeling part', dag=dag)

# modeling_email_task = EmailOperator(
#                 task_id='modeling_email_task',
#                 to=EMAIL_LIST,
#                 subject='Modeling part is successfully done! [1/2]',
#                 html_content='Modeling part => Transfering part', dag=dag)
#
welcome_email_task = EmailOperator(
                task_id='welcome_email_task',
                to=EMAIL_LIST,
                subject='Welcome Email',
                html_content='Welcome Email', dag=dag)
#
#

# modeling_email_task >> truncate_sell_amt_table_task

# for idx, item_part in enumerate(range(10)):
#
#     retrieve_dataset_from_presto_task = PythonOperator(task_id='retrieve_dataset_from_presto_task_%s' % item_part,
#                                 op_kwargs={'item_part': item_part},
#                                 python_callable=retrieve_dataset_from_presto,
#                                 priority_weight=1,
#                                 dag=dag)
#
#     apply_model_task = PythonOperator(task_id='apply_model_task_%s' % item_part,
#                                 op_kwargs={'item_part': item_part},
#                                 python_callable=apply_model_op,
#                                 priority_weight=2,
#                                 dag=dag)
#
#     transfer_to_mysql_temp_task = PythonOperator(task_id='transfer_to_mysql_temp_task_%s' % item_part,
#                                 op_kwargs={'item_part': item_part, 'engine': wspider_temp_engine},
#                                 python_callable=transfer_to_mysql_op,
#                                 dag=dag)
#
#     begin_task >> retrieve_dataset_from_presto_task >> apply_model_task >> modeling_email_task
#     truncate_sell_amt_table_task >> transfer_to_mysql_temp_task >> final_email_task

start = 0

genesis_task = DummyOperator(task_id='Genesis', dag=dag)

welcome_email_task >> genesis_task >> truncate_sell_amt_table_task

TOTAL_PARTITION = 400
N_CHUNCKS = 100

for end in range(0, TOTAL_PARTITION+1, N_CHUNCKS)[1:]:

    begin_task = DummyOperator(task_id='begin_part_%s_task' % end, dag=dag)
    success_email_task = EmailOperator(
        task_id='success_email_part_%s_task' % end,
        to=EMAIL_LIST,
        subject='Part %s is successfully done!' % end,
        html_content='Part %s is successfully done!' % end,
        dag=dag)




    modeling_email_task = EmailOperator(
        task_id='modeling_email_part_%s_task' % end,
        to=EMAIL_LIST,
        subject='Modeling part %s is successfully done!' % end,
        html_content='Modeling part %s is successfully done!' % end, dag=dag)

    for idx, item_part in enumerate(range(start, end)):


        retrieve_dataset_from_presto_task = PythonOperator(task_id='retrieve_dataset_from_presto_part_%s_task_%s' % (end, item_part),
                                    op_kwargs={'item_part': item_part},
                                    python_callable=retrieve_dataset_from_presto,
                                    priority_weight=1,
                                    dag=dag)

        apply_model_task = PythonOperator(task_id='apply_model_part_%s_task_%s' % (end, item_part),
                                    op_kwargs={'item_part': item_part},
                                    python_callable=apply_model_op,
                                    priority_weight=2,
                                    dag=dag)

        transfer_to_mysql_temp_task = PythonOperator(task_id='transfer_to_mysql_temp_part_%s_task_%s' % (end, item_part),
                                    op_kwargs={'item_part': item_part, 'engine': wspider_temp_engine},
                                    python_callable=transfer_to_mysql_op,
                                    dag=dag)

        begin_task >> retrieve_dataset_from_presto_task >> apply_model_task >> modeling_email_task >> transfer_to_mysql_temp_task >> success_email_task
    truncate_sell_amt_table_task >> begin_task

    start = end