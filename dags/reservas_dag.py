from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import pandas as pd
import requests
from typing import Dict, Any, List
import psycopg2
from dotenv import load_dotenv

dotenv_path = os.path.join(os.path.dirname(__file__), '..', 'config', '.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
    print(f".env cargado desde: {dotenv_path}")
else:
    raise FileNotFoundError(f"Archivo .env no encontrado en {dotenv_path}")

reservas_api_base_url = os.getenv("API_URL")
dolar_api_base_url = os.getenv("DOLAR_API_BASE_URL")

idVariable = 1
casa = "blue"

redshift_host = os.getenv("REDSHIFT_HOST")
redshift_port = os.getenv("REDSHIFT_PORT")
redshift_db = os.getenv("REDSHIFT_DB")
redshift_user = os.getenv("REDSHIFT_USER")
redshift_password = os.getenv("REDSHIFT_PASSWORD")
redshift_schema = os.getenv("REDSHIFT_SCHEMA")

missing_vars = [var for var in [reservas_api_base_url, dolar_api_base_url, redshift_host, redshift_port, redshift_db, redshift_user, redshift_password, redshift_schema] if var is None]
if missing_vars:
    raise ValueError(f"Una o más variables de entorno no están definidas correctamente: {missing_vars}")

redshift_port = int(redshift_port)

def guardar_en_redshift(df: pd.DataFrame, table_name: str):
    conn = psycopg2.connect(
        host=redshift_host,
        port=redshift_port,
        dbname=redshift_db,
        user=redshift_user,
        password=redshift_password
    )
    cursor = conn.cursor()
    cursor.execute(f'SET search_path TO "{redshift_schema}";')
    
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INT IDENTITY(1,1) PRIMARY KEY,
        {', '.join([f"{col} VARCHAR" for col in df.columns])}
    );
    """
    cursor.execute(create_table_query)
    
    for _, row in df.iterrows():
        insert_query = f"""
        INSERT INTO {table_name} ({', '.join(df.columns)})
        VALUES ({', '.join(['%s'] * len(row))});
        """
        cursor.execute(insert_query, tuple(row))
    
    conn.commit()
    cursor.close()
    conn.close()

def obtener_reservas(**kwargs):
    execution_date = kwargs['execution_date']
    desde = execution_date.strftime("%Y-%m-%d")
    hasta = execution_date.strftime("%Y-%m-%d")
    
    reservas_api_url = f"{reservas_api_base_url}/{idVariable}/{desde}/{hasta}"
    try:
        response = requests.get(reservas_api_url, verify=False)
        if response.status_code == 200:
            data_reservas = response.json()
            if 'results' in data_reservas:
                df_reservas = pd.DataFrame(data_reservas['results'])
                kwargs['ti'].xcom_push(key='df_reservas', value=df_reservas.to_dict())
                guardar_en_redshift(df_reservas, "reservas")
            else:
                raise ValueError("No se encontraron resultados en la respuesta de la API de reservas.")
        else:
            raise ValueError(f"Error en la solicitud a la API de reservas: {response.status_code}")
    except Exception as e:
        raise Exception(f"Excepción durante la solicitud a la API de reservas: {e}")

def obtener_dolar(**kwargs):
    execution_date = kwargs['execution_date']
    
    fecha = execution_date.strftime("%Y/%m/%d")
    datos_dolar = []
    try:
        dolar_api_url = f"{dolar_api_base_url}/{casa}/{fecha}"
        response = requests.get(dolar_api_url, verify=False)
        if response.status_code == 200:
            datos_dia = response.json()
            if datos_dia:
                datos_dolar.append(datos_dia)
        else:
            print(f"Error en la solicitud del dólar para {fecha}: {response.status_code}")
        df_dolar = pd.DataFrame(datos_dolar)
        kwargs['ti'].xcom_push(key='df_dolar', value=df_dolar.to_dict())
        guardar_en_redshift(df_dolar, "dolar")
    except Exception as e:
        raise Exception(f"Excepción durante la solicitud a la API de dólar: {e}")

def calcular_reservas_en_pesos(**kwargs):
    df_reservas = pd.DataFrame(kwargs['ti'].xcom_pull(key='df_reservas'))
    df_dolar = pd.DataFrame(kwargs['ti'].xcom_pull(key='df_dolar'))

    df_combined = pd.merge(df_reservas, df_dolar, on='fecha', how="inner", validate="many_to_many")
    
    df_combined.rename(columns={'valor': 'reservas_dolares', 'venta': 'valor_blue'}, inplace=True)
    
    df_combined['reservas_en_pesos'] = df_combined['reservas_dolares'] * df_combined['valor_blue']

    print(df_combined[['fecha', 'reservas_dolares', 'valor_blue', 'reservas_en_pesos']])
    guardar_en_redshift(df_combined[['fecha', 'reservas_dolares', 'valor_blue', 'reservas_en_pesos']], "reservas_en_pesos")

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