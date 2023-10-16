from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta 
from airflow.operators.postgres_operator import PostgresOperator


dag = DAG('etl_postgres',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2021, 12, 17, 0)
    )

check_age = PostgresOperator(
        task_id='check_age',
        postgres_conn_id='postgres_default',
        dag = dag,
        sql="""SELECT * FROM "HR_MI086_TEST".dm_gph;"""
)

check_age_e = PostgresOperator(
        task_id='check_age_e',
        postgres_conn_id='postgres_default',
        dag = dag,
        sql="SELECT CASE WHEN 22 > 21 THEN 'adult' ELSE 'young' END"
)

check_age >> check_age_e