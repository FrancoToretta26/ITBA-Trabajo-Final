from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from etl.api_request import obtener_reservas, obtener_dolar
from etl.redshift_save import guardar_en_redshift
from etl.transformations import calcular_reservas_en_pesos

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'reservas_dag',
    default_args=default_args,
    description='DAG para obtener reservas y cotización del dólar',
    schedule_interval='@daily',
)

tarea_obtener_reservas = PythonOperator(
    task_id='obtener_reservas',
    python_callable=obtener_reservas,
    provide_context=True,
    retries=3,
    retry_delay=timedelta(minutes=5),
    dag=dag,
)

tarea_obtener_dolar = PythonOperator(
    task_id='obtener_dolar',
    python_callable=obtener_dolar,
    provide_context=True,
    retries=3,
    retry_delay=timedelta(minutes=5),
    dag=dag,
)

tarea_calcular_reservas = PythonOperator(
    task_id='calcular_reservas_en_pesos',
    python_callable=calcular_reservas_en_pesos,
    provide_context=True,
    dag=dag,
)

tarea_obtener_reservas >> tarea_obtener_dolar >> tarea_calcular_reservas