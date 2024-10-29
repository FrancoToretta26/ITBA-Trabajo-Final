import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv

dotenv_path = os.path.join(os.path.dirname(__file__), '../..', 'config', '.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

redshift_host = os.getenv("REDSHIFT_HOST")
redshift_port = os.getenv("REDSHIFT_PORT")
redshift_db = os.getenv("REDSHIFT_DB")
redshift_user = os.getenv("REDSHIFT_USER")
redshift_password = os.getenv("REDSHIFT_PASSWORD")
redshift_schema = os.getenv("REDSHIFT_SCHEMA")

redshift_port = int(redshift_port)

def guardar_en_redshift(df: pd.DataFrame, table_name: str):
    """
    Guarda un DataFrame en una tabla de Redshift, asegurando que la operación sea idempotente (eliminando las fechas que ya existan).

    Args:
        df (pd.DataFrame): DataFrame a guardar en Redshift.
        table_name (str): Nombre de la tabla en Redshift.

    Raises:
        psycopg2.Error: Si ocurre un error al interactuar con la base de datos.
    """
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
    
    fechas = df['fecha'].unique()
    fechas_str = ', '.join([f"'{fecha}'" for fecha in fechas])
    delete_query = f"DELETE FROM {table_name} WHERE fecha IN ({fechas_str});"
    cursor.execute(delete_query)
    
    for _, row in df.iterrows():
        insert_query = f"""
        INSERT INTO {table_name} ({', '.join(df.columns)})
        VALUES ({', '.join(['%s'] * len(row))});
        """
        cursor.execute(insert_query, tuple(row))
    
    conn.commit()
    cursor.close()
    conn.close()