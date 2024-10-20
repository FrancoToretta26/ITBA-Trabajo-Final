import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from pandas.testing import assert_frame_equal
from dags.etl.transformations import calcular_reservas_en_pesos

class TestTransformations(unittest.TestCase):

    @patch('dags.etl.transformations.guardar_en_redshift')
    def test_calcular_reservas_en_pesos(self, mock_guardar_en_redshift):
        df_reservas = pd.DataFrame({
            'fecha': ['2024-07-01'],
            'valor': [100]
        })
        df_dolar = pd.DataFrame({
            'fecha': ['2024-07-01'],
            'venta': [150]
        })

        kwargs = {
            'ti': MagicMock()
        }
        kwargs['ti'].xcom_pull.side_effect = lambda key: df_reservas if key == 'df_reservas' else df_dolar

        calcular_reservas_en_pesos(**kwargs)

        self.assertTrue(mock_guardar_en_redshift.called)


        args, _ = mock_guardar_en_redshift.call_args

        # DataFrame esperado
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
