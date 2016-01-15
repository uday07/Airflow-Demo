#@auther: uday sharma

from airflow import DAG
from airflow.operators import BashOperator, HiveOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'udaysharma',
    'start_date': datetime(2016, 1, 14),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('incremental_load', default_args=default_args)

sqoop_job = """
 exec ./scripts/sqoop_incremental.sh
"""
# Importing the data from Mysql table to HDFS
task1 = BashOperator(
        task_id= 'sqoop_import',
        bash_command=sqoop_job,
        dag=dag
)

# Inserting the data from Hive external table to the target table
task2 = HiveOperator(
        task_id= 'hive_insert',
        hql='INSERT INTO TABLE orders_trans SELECT order_id, first_name,last_name, item_code, order_date FROM orders_stg;',
        depends_on_past=True,
        dag=dag
)

# defining the job dependency
task2.set_upstream(task1)