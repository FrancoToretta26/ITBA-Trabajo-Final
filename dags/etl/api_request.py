import requests
import pandas as pd
import os
from dotenv import load_dotenv
from .redshift_save import guardar_en_redshift

dotenv_path = os.path.join(os.path.dirname(__file__), '../..', 'config', '.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

reservas_api_base_url = os.getenv("API_URL")
dolar_api_base_url = os.getenv("DOLAR_API_BASE_URL")

idVariable = 1
casa = "blue"

def obtener_reservas(**kwargs):
    """
    Obtiene las reservas desde una API y guarda los datos en un archivo CSV.

    Args:
        **kwargs: Diccionario de argumentos que incluye 'execution_date' y 'ti' (Task Instance).

    Raises:
        ValueError: Si no se encuentran resultados en la respuesta de la API de reservas.
        Exception: Si ocurre una excepción durante la solicitud a la API de reservas.
    """
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
                file_path = f"/tmp/reservas_{desde}_{hasta}.csv"
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                df_reservas.to_csv(file_path, index=False)
                kwargs['ti'].xcom_push(key='reservas_file_path', value=file_path)
            else:
                raise ValueError("No se encontraron resultados en la respuesta de la API de reservas.")
        else:
            raise ValueError(f"Error en la solicitud a la API de reservas: {response.status_code}")
    except Exception as e:
        raise Exception(f"Excepción durante la solicitud a la API de reservas: {e}")

def obtener_dolar(**kwargs):
    """
    Obtiene la cotización del dólar desde una API y guarda los datos en un archivo CSV.

    Args:
        **kwargs: Diccionario de argumentos que incluye 'execution_date' y 'ti' (Task Instance).

    Raises:
        Exception: Si ocurre una excepción durante la solicitud a la API de dólar.
    """
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
        file_path = f"/tmp/dolar_{fecha}.csv"
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        df_dolar.to_csv(file_path, index=False)
        kwargs['ti'].xcom_push(key='dolar_file_path', value=file_path)
    except Exception as e:
        raise Exception(f"Excepción durante la solicitud a la API de dólar: {e}")