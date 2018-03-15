# from __future__ import print_function
# import airflow
# from airflow import DAG
# import time
# import pandas as pd
# from airflow.operators.python_operator import PythonOperator
# from airflow.hooks.presto_hook import PrestoHook
# from airflow.hooks.mysql_hook import MySqlHook
#
# from airflow.hooks.hive_hooks import HiveServer2Hook
#
# args = {
#     'owner': 'airflow',
#     'start_date': airflow.utils.dates.days_ago(2),
#     'provide_context': True
# }
#
# dag = DAG(
#     'example_xcom_pandas',
#     schedule_interval="@once",
#     default_args=args)
#
# value_1 = pd.DataFrame([{'name': 'A', 'age':24}, {'name': 'B', 'age': 27}])
# value_2 = {'a': 'b'}
#
#
# def push(**kwargs):
#     # pushes an XCom without a specific target
#     kwargs['ti'].xcom_push(key='value from pusher 1', value=value_1)
#
#
# def push_by_returning(**kwargs):
#     # pushes an XCom without a specific target, just by returning it
#     return value_2
#
#
# def puller(**kwargs):
#     ti = kwargs['ti']
#
#     # get value_1
#     v1 = ti.xcom_pull(key=None, task_ids='push')
#     print(v1)
#
#     # get value_2
#     v2 = ti.xcom_pull(task_ids='push_by_returning')
#     print(v2)
#
#     # get both value_1 and value_2
#     v1, v2 = ti.xcom_pull(key=None, task_ids=['push', 'push_by_returning'])
#     print(v1, v2)
#
#     presto = PrestoHook()
#     mysql = MySqlHook()
#     start = time.time()
#     impala = HiveServer2Hook()
#     results = mysql.get_pandas_df(sql="SELECT * FROM wspider_temp.MWS_COLT_ITEM_IVT WHERE ITEM_ID in (11012, 11013, 11014, 11015, 11016, 11017)")
#     # results = presto.get_pandas_df(hql="SELECT * FROM inventory WHERE item_id = 7451430")
#     # print(results)
#
#     print(time.time() - start)
#
#
# push1 = PythonOperator(
#     task_id='push', dag=dag, python_callable=push)
#
# push2 = PythonOperator(
#     task_id='push_by_returning', dag=dag, python_callable=push_by_returning)
#
# pull = PythonOperator(
#     task_id='puller', dag=dag, python_callable=puller)
#
# pull.set_upstream([push1, push2])