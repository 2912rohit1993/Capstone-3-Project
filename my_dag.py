import os
import datetime 
from airflow import DAG
from airflow import models
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

with DAG(
    'postgres_to_bq',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Capstone-3 DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
	task1=BashOperator(
		task_id='Loading_dim_address',
		bash_command="python /home/airflow/gcs/dags/dim_address_script.py"
	)
	task2=BashOperator(
		task_id='Loading_dim_customer',
		bash_command="python /home/airflow/gcs/dags/dim_customer_script.py"
	)
	task3=BashOperator(
		task_id='Loading_dim_order',
		bash_command="python /home/airflow/gcs/dags/dim_order_script.py"
	)
	task4=BashOperator(
		task_id='Loading_dim_product',
		bash_command="python /home/airflow/gcs/dags/dim_product_script.py"
	)    
	task5=BashOperator(
		task_id='Loading_fact_daily_orders',
		bash_command="python /home/airflow/gcs/dags/fact_daily_orders_script.py"
	)
 	task6=BashOperator(
		task_id='Loading_fact_order_details',
		bash_command="python /home/airflow/gcs/dags/fact_order_details_script.py"
	)
	task1 >> task1 >> task1 >> task4 >> task5 >> task6
