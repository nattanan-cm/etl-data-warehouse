 

# import os
from io import StringIO
import pandas as pd
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

default_args = {
    'owner': 'nattanan-cm',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

BUCKET_NAME = 'bronze'
PREFIX = 'chao-phraya-water-quality'
S3_CONNECTION_ID = 's3_conn'
PG_CONNECTION_ID = 'pg_data_warehouse_conn'
NAME = 'chaophraya'

@dag(
   dag_id='water_quality_etl',
   default_args=default_args,
   start_date=datetime(2024, 10, 30),
   schedule_interval=None,
   tags=['water-quality', 'chao-phraya', 'etl', 'souce: data-warehouse', 'csv', 'store: postgres']
)


# MinIO can be used as a data lake or a component of a data lake architecture.
def water_quality_etl():
   s3_hook = S3Hook(aws_conn_id=S3_CONNECTION_ID)
      
   @task()
   def extract():
      # List all files in the specified folder
      file_keys = s3_hook.list_keys(bucket_name=BUCKET_NAME, prefix=PREFIX)
      
      print(file_keys)
    
      
      key_list = [key for key in file_keys if key.endswith(".csv") and NAME in key]

      return key_list
         
   @task()
   def transform(list: list[str]):
      data = []
      
      for key in list:
         obj = s3_hook.get_key(key=key, bucket_name=BUCKET_NAME)
         csv_content = obj.get()['Body'].read().decode('utf-8')
         df = pd.read_csv(StringIO(csv_content))
         
         print(f'{NAME} has : {len(df)} records')
         
         df['depth'].fillna(df['depth'].mean(), inplace=True)
         df['ph'].fillna(df['ph'].mean(), inplace=True)
         df['depth'].fillna(df['depth'].mean(), inplace=True)
         df['turbid'].fillna(df['turbid'].mean(), inplace=True)
         df['nh4'].fillna(df['nh4'].mean(), inplace=True)
         df['deo'].fillna(df['deo'].mean(), inplace=True)
         df['datetimes'] = pd.to_datetime(df['datetimes'])
         df['datetimes'].dt.strftime('%Y/%m/%d %H:%M:%S')
         
         for _, row in df.iterrows():
            # Convert the date column to datetime format
            json = {
               "timestamp": str(row['datetimes']),
               "station_id": str(row['stn_id']),
               "station_name": str(row['stn_name']),
               "temp": float(row['temp']),
               "conducted": float(row['conducted']),
               "tds": float(row['tds']),
               "salinity": float(row['salinity']),
               "depth": float(row['depth']),
               "deo": float(row['deo']),
               "ph": float(row['ph']),
               "nh4": float(row['nh4']),
               "turbid": float(row['turbid']),
            }      
         
            data.append(json)
            
      return data
      
   @task()
   def load(data: list[dict]):
      pg_hook = PostgresHook(postgres_conn_id=PG_CONNECTION_ID)
      
      keys = data[0].keys()
      
      insert_data = [tuple(row[key] for key in keys) for row in data]
      
      pg_hook.insert_rows(
        table="chao_phraya_water_quality",
        rows=insert_data,
        target_fields=[", ".join(key for key in keys)]
    )
      
   csv_list = extract()
   data = transform(csv_list)
   load(data)
   
water_quality_etl_dag = water_quality_etl()