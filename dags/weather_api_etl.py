from airflow import DAG
from datetime import datetime, timedelta
from airflow.decorators import dag, task

default_args = {
   'owner': 'nattanan-cm',
   'retries': 5,
   'retry_delay': timedelta(minutes=2)
}

@dag(
   dag_id='weather_etl',
   default_args=default_args,
   start_date=datetime(2024,7,31),
   schedule="@hourly",
)

def weather_etl():
   
   @task()
   def extract():
      pass
   
   @task(multiple_outputs=True) # When multiple_outputs=True is set, the function can return a dictionary with multiple key-value pairs. 
   def transform(data: dict):
      pass
   
   @task()
   def load(data: dict):
      pass
   
   json = extract()
   data = transform(json)
   load(data)
   
weather_etl_dag = weather_etl()