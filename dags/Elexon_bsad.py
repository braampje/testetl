from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'bram',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2019, 1, 1),
    'execution_timeout': timedelta(hours=1),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'Elexon_bsad', default_args=default_args,
    schedule_interval='@daily')

t1 = BashOperator(
    task_id='scraper',
    bash_command='cd /home/bram/testetl/scrapy && scrapy crawl Elexon_bsad -a STARTDATE={{ ds }}',
    dag=dag)

t2 = BashOperator(
    task_id='dumper',
    bash_command='cd /home/bram/testetl/Main && python Elexon_bsad.py {{ ds }}',
    dag=dag)

t1 >> t2
