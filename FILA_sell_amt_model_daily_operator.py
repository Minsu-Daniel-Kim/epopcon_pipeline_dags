
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
    n_item_ids = len(param['item_ids'])
    item_id = param['item_ids'][0]
    item_ids = str(tuple(param['item_ids']))

    if DEVELOPMENT:
        if n_item_ids == 1:
            hql = "SELECT ID, ITEM_ID, STOCK_ID, STOCK_AMOUNT, COLLECT_DAY, REG_ID, REG_DT FROM inventory_part WHERE item_part = {} AND item_id = {} LIMIT 500".format(partition, item_id)
        else:
            hql = "SELECT ID, ITEM_ID, STOCK_ID, STOCK_AMOUNT, COLLECT_DAY, REG_ID, REG_DT FROM inventory_part WHERE item_part = {} AND item_id IN {} LIMIT 500".format(partition, item_ids)

    else:
        if n_item_ids == 1:
            hql = "SELECT ID, ITEM_ID, STOCK_ID, STOCK_AMOUNT, COLLECT_DAY, REG_ID, REG_DT FROM inventory_part WHERE item_part = {} AND item_id = {} ".format(partition, item_id)
        else:
            hql = "SELECT ID, ITEM_ID, STOCK_ID, STOCK_AMOUNT, COLLECT_DAY, REG_ID, REG_DT FROM inventory_part WHERE item_part = {} AND item_id IN {} ".format(partition, item_ids)

    result = presto.get_pandas_df(hql=hql)

    result.columns = ["ID", "ITEM_ID", "STOCK_ID", "STOCK_AMOUNT", "COLLECT_DAY", "REG_ID", "REG_DT"]
    result.REG_DT = pd.to_datetime(result.REG_DT)

    logger.info("time to retrieve partition %s from server...." % partition)
    write_to_feather(partition, result, processed=False)
    logger.info("retrieving partition %s done" % partition)

def retrieve_item_ids():
    import datetime
    today = datetime.date.today()
    week_of_year = today.isocalendar()[1]
    query_date = datetime.date.today() - timedelta(days=5)

    hql = "SELECT ID FROM ITEM_PART WHERE WEEK_PART IN ({}, {}) AND UPT_DT >= '{}' AND SITE_NAME IN ('NIKE', 'DESCENTE', 'nbkorea', 'adidas')".format(
        week_of_year - 1, week_of_year, query_date)
    result = presto.get_pandas_df(hql=hql)
    result.columns = ['ITEM_ID']
    result = result.assign(ITEM_PART=(result.ITEM_ID // 10000) + 1)
    # result_dict = result.groupby('ITEM_PART')['ITEM_ID'].apply(lambda group: group.values).to_dict()

    write_to_feather(None, result, meta=True)

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
    engine.dispose()
    insert_sell_amt_old(dataset)

def remove_meta_file_op():
    remove_meta_file()


presto = PrestoHook()


DEVELOPMENT = False

if DEVELOPMENT:
    engine = wspider_engine

else:
    engine = wspider_temp_engine

EMAIL_LIST = [
                'daniel.kim@epopcon.com'
                ,'zururux@epopcon.com'
]

dag = DAG('FILA_sell_amt_daily_modeling_dag_production2',
          default_args=default_args,
          dagrun_timeout=timedelta(days=2),
          description='Modeling the sell amount',
          # schedule_interval='0 0 * * 0',
          schedule_interval='0 3 * * *',
          # schedule_interval="@once",
          catchup=False)

begin_task = DummyOperator(task_id='begin_task', dag=dag)


success_email_task = EmailOperator(
                task_id='success_email_task',
                to=EMAIL_LIST,
                subject='Success!!',
                html_content='Success Email', dag=dag)

remove_meta_file_task = PythonOperator(task_id='remove_meta_file_task',
                                    python_callable=remove_meta_file_op,
                                    dag=dag)

success_email_task >> remove_meta_file_task

path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
filename = "item_ids.feather"
complete_filename = os.path.join(path, "tmp_feathers/meta", filename)

if not os.path.exists(complete_filename):
    retrieve_item_ids()

else:

    result = read_feather(None, meta=True)
    result_dict = result.groupby('ITEM_PART')['ITEM_ID'].apply(lambda group: group.values).to_dict()
    # print(result_dict)

    for key, value in result_dict.items():

        retrieve_dataset_from_presto_task = PythonOperator(task_id='retrieve_dataset_from_presto_task_{}'.format(key),
                                    op_kwargs={'item_part': key, 'item_ids': value},
                                    python_callable=retrieve_dataset_from_presto,
                                    # priority_weight=1,
                                    dag=dag)

        apply_model_task = PythonOperator(task_id='apply_model_task_{}'.format(key),
                                      op_kwargs={'item_part': key},
                                      python_callable=apply_model_op,
                                      # priority_weight=2,
                                      dag=dag)

        transfer_to_mysql_temp_task = PythonOperator(task_id='transfer_to_mysql_temp_task_{}'.format(key),
                                    op_kwargs={'item_part': key, 'engine': engine},
                                    python_callable=transfer_to_mysql_op,
                                    # priority_weight=5,
                                    dag=dag)


        begin_task >> retrieve_dataset_from_presto_task >> apply_model_task >> transfer_to_mysql_temp_task >> success_email_task


# retrieve_item_ids_task = PythonOperator(task_id='retrieve_item_ids_task',
#                python_callable=retrieve_item_ids,
#                dag=dag)



# truncate_sell_amt_table_task = PythonOperator(
#                             task_id='truncate_sell_amt_table_task',
#                             python_callable=truncate_sell_amt_table_op,
#                             dag=dag)

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


#
# start = 0
#
# genesis_task = DummyOperator(task_id='Genesis', dag=dag)
#
#
#
# TOTAL_PARTITION = 400
# N_CHUNCKS = 100
#
# for end in range(0, TOTAL_PARTITION+1, N_CHUNCKS)[1:]:
#
#     begin_task = DummyOperator(task_id='begin_part_%s_task' % end, dag=dag)
#     success_email_task = EmailOperator(
#         task_id='success_email_part_%s_task' % end,
#         to=EMAIL_LIST,
#         subject='Part %s is successfully done!' % end,
#         html_content='Part %s is successfully done!' % end,
#         dag=dag)
#
#
#
#
#     modeling_email_task = EmailOperator(
#         task_id='modeling_email_part_%s_task' % end,
#         to=EMAIL_LIST,
#         subject='Modeling part %s is successfully done!' % end,
#         html_content='Modeling part %s is successfully done!' % end, dag=dag)
#
#     for idx, item_part in enumerate(range(start, end)):
#
#
#         retrieve_dataset_from_presto_task = PythonOperator(task_id='retrieve_dataset_from_presto_part_%s_task_%s' % (end, item_part),
#                                     op_kwargs={'item_part': item_part},
#                                     python_callable=retrieve_dataset_from_presto,
#                                     priority_weight=1,
#                                     dag=dag)
#
#         apply_model_task = PythonOperator(task_id='apply_model_part_%s_task_%s' % (end, item_part),
#                                     op_kwargs={'item_part': item_part},
#                                     python_callable=apply_model_op,
#                                     priority_weight=2,
#                                     dag=dag)
#
#         transfer_to_mysql_temp_task = PythonOperator(task_id='transfer_to_mysql_temp_part_%s_task_%s' % (end, item_part),
#                                     op_kwargs={'item_part': item_part, 'engine': wspider_temp_engine},
#                                     python_callable=transfer_to_mysql_op,
#                                     dag=dag)
#
#         begin_task >> retrieve_dataset_from_presto_task >> apply_model_task >> modeling_email_task >> transfer_to_mysql_temp_task >> success_email_task
#     truncate_sell_amt_table_task >> begin_task
#
#     start = end