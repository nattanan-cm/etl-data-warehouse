from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'nattanan-cm',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

CONNECTION_ID = 'pg_data_warehouse_conn'
FACT_TABLE_NAME = 'fact_chao_phraya_water_quality_2023_v1'
SOURCE_TABLE_NAME = 'chao_phraya_water_quality'

with DAG(
   dag_id='fact_water_quality_2023_v1',
   default_args=default_args,
   start_date=datetime(2024, 10, 30),
   schedule_interval='None',
   tags=['water-quality', 'chao-phraya', 'partition', 'souce: postgres', 'store: postgres']
) as dag:
   create_tb_task = PostgresOperator(
      task_id='create_fact_table',
      postgres_conn_id=CONNECTION_ID,
      sql=f"""
         CREATE TABLE {FACT_TABLE_NAME}
         (LIKE {SOURCE_TABLE_NAME} INCLUDING ALL);
      """
   )
   
   partition_task = PostgresOperator(
      task_id='partitioning',
      postgres_conn_id=CONNECTION_ID,
      sql=f"""
         INSERT INTO {FACT_TABLE_NAME} 
         SELECT *
         FROM {SOURCE_TABLE_NAME}
         WHERE date_part('year', timestamp) = '2023';
      """
   )

   create_tb_task >> partition_task