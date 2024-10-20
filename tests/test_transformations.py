import unittest
from unittest.mock import MagicMock
import pandas as pd
from dags.etl.transformations import calcular_reservas_en_pesos

class TestTransformations(unittest.TestCase):

    def test_calcular_reservas_en_pesos(self):
        df_reservas = pd.DataFrame({'fecha': ['2024-07-01'], 'reservas_dolares': [100]})
        df_dolar = pd.DataFrame({'fecha': ['2024-07-01'], 'valor_blue': [150]})

        kwargs = {'ti': MagicMock()}
        kwargs['ti'].xcom_pull.side_effect = [df_reservas.to_dict(), df_dolar.to_dict()]

        calcular_reservas_en_pesos(**kwargs)

        df_combined = pd.merge(df_reservas, df_dolar, on='fecha', how="inner", validate="many_to_many")
        df_combined['reservas_en_pesos'] = df_combined['reservas_dolares'] * df_combined['valor_blue']

        expected_output = df_combined[['fecha', 'reservas_dolares', 'valor_blue', 'reservas_en_pesos']]
        print(expected_output)

if __name__ == '__main__':
    unittest.main()