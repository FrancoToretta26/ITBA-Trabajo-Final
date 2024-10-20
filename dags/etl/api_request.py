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