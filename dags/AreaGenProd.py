from airflow import DAG
from airflow.operators.bash_operator import bashOperator
from datetime import datetime, timedelta

default_args = {
	'owner': 'bram',
	'depends_on_past': False,
	'retries': 2,
	'retry_delay': timedelta(minutes=5),
	'start_date': datetime(2017, 1, 1),
	'execution_timeout': timedelta(hours=1),
	'email_on_failure': False,
	'email_on_retry': False,
}


dag = DAG(
	'Elexon_Areagen_actual', default_args=default_args, schedule_interval(1))

t1 = bashOperator(
	task_id='scraper',
	bash_command='/home/bram/testetl/scrapy/scrapy crawl AreaGen -a STARTDATE=%(ds)s',
	dag=dag)

t2 = bashOperator(
	task_id='dumper',
	bash_command='/home/bram/testetl/Main/python AreaGen_actual %(ds)s',
	dag=dag)

t1 >> t2
