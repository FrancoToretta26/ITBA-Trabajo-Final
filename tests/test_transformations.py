import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from pandas.testing import assert_frame_equal
from dags.etl.transformations import calcular_reservas_en_pesos

class TestTransformations(unittest.TestCase):

    @patch('dags.etl.transformations.pd.read_csv')
    @patch('dags.etl.transformations.guardar_en_redshift')
    def test_calcular_reservas_en_pesos(self, mock_guardar_en_redshift, mock_read_csv):
        df_reservas = pd.DataFrame({
            'fecha': ['2024-07-01'],
            'valor': [100]
        })
        df_dolar = pd.DataFrame({
            'fecha': ['2024-07-01'],
            'venta': [150]
        })

        mock_read_csv.side_effect = [df_reservas, df_dolar]

        kwargs = {
            'ti': MagicMock()
        }
        kwargs['ti'].xcom_pull.side_effect = lambda key: '/tmp/reservas_2024-07-01_2024-07-01.csv' if key == 'reservas_file_path' else '/tmp/dolar_2024-07-01.csv'

        calcular_reservas_en_pesos(**kwargs)

        self.assertTrue(mock_guardar_en_redshift.called)

        args, _ = mock_guardar_en_redshift.call_args

        expected_df = pd.DataFrame({
            'fecha': ['2024-07-01'],
            'reservas_dolares': [100],
            'valor_blue': [150],
            'reservas_en_pesos': [15000]
        })

        assert_frame_equal(args[0], expected_df)

        self.assertEqual(args[1], "reservas_en_pesos")

if __name__ == '__main__':
    unittest.main()