import pandas as pd
from .redshift_save import guardar_en_redshift

def calcular_reservas_en_pesos(**kwargs):
    """
    Calcula las reservas en pesos a partir de las reservas en dólares y la cotización del dólar,
    y guarda los resultados en Redshift.

    Args:
        **kwargs: Diccionario de argumentos que incluye 'ti' (Task Instance).

    Raises:
        KeyError: Si no se encuentran las claves 'reservas_file_path' o 'dolar_file_path' en XCom.
    """
    ti = kwargs['ti']
    reservas_file_path = ti.xcom_pull(key='reservas_file_path')
    dolar_file_path = ti.xcom_pull(key='dolar_file_path')

    df_reservas = pd.read_csv(reservas_file_path)
    df_dolar = pd.read_csv(dolar_file_path)

    df_combined = pd.merge(df_reservas, df_dolar, on='fecha', how="inner", validate="many_to_many")
    
    df_combined.rename(columns={'valor': 'reservas_dolares', 'venta': 'valor_blue'}, inplace=True)
    
    df_combined['reservas_en_pesos'] = df_combined['reservas_dolares'] * df_combined['valor_blue']

    print(df_combined[['fecha', 'reservas_dolares', 'valor_blue', 'reservas_en_pesos']])
    guardar_en_redshift(df_combined[['fecha', 'reservas_dolares', 'valor_blue', 'reservas_en_pesos']], "reservas_en_pesos")