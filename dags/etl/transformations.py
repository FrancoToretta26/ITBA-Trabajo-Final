import pandas as pd
from .redshift_save import guardar_en_redshift

def calcular_reservas_en_pesos(**kwargs):
    """
    Calcula las reservas en pesos a partir de las reservas en dólares y la cotización del dólar blue,
    y guarda los resultados en Redshift.

    Args:
        **kwargs: Diccionario de argumentos que incluye 'ti' (Task Instance).

    Raises:
        KeyError: Si no se encuentran las claves 'df_reservas' o 'df_dolar' en XCom.
    """

    df_reservas = pd.DataFrame(kwargs['ti'].xcom_pull(key='df_reservas'))
    df_dolar = pd.DataFrame(kwargs['ti'].xcom_pull(key='df_dolar'))

    df_combined = pd.merge(df_reservas, df_dolar, on='fecha', how="inner", validate="many_to_many")
    
    df_combined.rename(columns={'valor': 'reservas_dolares', 'venta': 'valor_blue'}, inplace=True)
    
    df_combined['reservas_en_pesos'] = df_combined['reservas_dolares'] * df_combined['valor_blue']

    print(df_combined[['fecha', 'reservas_dolares', 'valor_blue', 'reservas_en_pesos']])
    guardar_en_redshift(df_combined[['fecha', 'reservas_dolares', 'valor_blue', 'reservas_en_pesos']], "reservas_en_pesos")