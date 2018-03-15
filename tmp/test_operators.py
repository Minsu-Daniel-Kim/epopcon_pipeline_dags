# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators import MyFirstOperator, EmailOperator, BashOperator, ManagerOperator, PrinterOperator, DataRetriveOperator
# from airflow.operators.mysql_operator import MySqlOperator
# from airflow.hooks.hive_hooks import HiveServer2Hook
# from airflow.hooks.mysql_hook import MySqlHook
#
# # from airflow.operators import MySqlOperator
#
# import numpy as np
#
# default_args = {
#     'owner': 'Daniel',
#     'depends_on_past': False,
#     'start_date': datetime(2018, 3, 5),
#     'email_on_failure': True,
#     'email_on_retry': True,
#     'retries': 5,
#     'retry_delay': timedelta(minutes=1),
# }
#
# dag = DAG('Daniel_task2', default_args=default_args, description='Another tutorial DAG',
#           schedule_interval='0 12 * * *', catchup=False)
#
# mysql_hook = MySqlHook(conn_id='dfdfd_msql')
#
# # ids = mysql_hook.get_pandas_df('SELECT ID FROM MWS_COLT_ITEM LIMIT 100')
#
# # t = MySqlOperator(task_id='mysql_task', sql='SELECT * FROM wspider_temp.MWS_COLT_ITEM_IVT LIMIT 110', dag=dag)
# # t.run(start_date=datetime(2018, 3, 5), end_date=datetime(2018, 3, 8), ignore_ti_state=True)
#
#
# # dummy_task = DummyOperator(task_id='dummy_task', dag=dag)
#
# # sensor_task = MyFirstSensor(task_id='my_sensor_task', poke_interval=30, dag=dag)
#
#
# manager_task = ManagerOperator(my_operator_param='test', db_hook=mysql_hook, task_id='manager_task', dag=dag)
#
#
#
# impala_task = DataRetriveOperator(my_operator_param='impala', db_hook=mysql_hook, task_id='my_impala_task', dag=dag)
#
#
# manager_task >> impala_task
#
# # manager_task >> impala_task
# # db_tasks = []
# # for i in range(10):
# #     db_task = DataRetriveOperator(my_operator_param='impala', db_hook=mysql_hook, task_id='my_impala_task_%s' % i, dag=dag)
# #     db_tasks.append(db_task)
# #
# # for db_task in db_tasks:
# #     dummy_task >> db_task
#
# # mysql_task = MySqlOperator(task_id='mysql_task', sql='SELECT * FROM airflow.connection', dag=dag)
# # print(mysql_task)
#
# # mysql_task = MySqlOperator(task_id='mysql_task', sql='SELECT * FROM wspider_temp.ADDRESS', dag=dag)
#
# # printer_task = PrinterOperator(my_operator_param='printer', )
#
# # operator_task = MyFirstOperator(my_operator_param='This is a test', task_id='my_first_operator_task', dag=dag)
#
#
# # emailNotify_task = EmailOperator(
# # 			task_id='email_notification',
# # 			to = ['daniel.kim@epopcon.com'],
# # 			subject = 'Epopcon ETL Daily Report',
# # 			html_content = 'Epopcon ETL is successfully finished!', dag=dag)
#
# # dummy_task >> sensor_task >> manager_task
#
# # arr_lst = np.array(range(100))
# #
# # jobs = np.array_split(arr_lst, 20)
# #
# # print_tasks = []
# #
# # for idx, job in enumerate(jobs):
# #     task = PrinterOperator(my_operator_param='printer', task_id='printer_%s' % idx, assigned_lst=job, dag=dag)
# #     print_tasks.append(task)
# #
#
# # manager_task >> sensor_task >> emailNotify_task
#
# # manager_task >> mysql_task
# # dummy_task >> mysql_task
#
# # for print_task in print_tasks:
# #     dummy_task >> print_task >> emailNotify_task