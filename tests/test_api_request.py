from datetime import datetime
import unittest
from unittest.mock import patch, MagicMock
import os
import pandas as pd
from dags.etl.api_request import obtener_reservas, obtener_dolar

class TestApiRequest(unittest.TestCase):

    @patch('dags.etl.api_request.requests.get')
    @patch('dags.etl.api_request.os.makedirs')
    @patch('dags.etl.api_request.pd.DataFrame.to_csv')
    def test_obtener_reservas(self, mock_to_csv, mock_makedirs, mock_requests_get):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'results': [{'fecha': '2024-07-01', 'valor': 100}]}
        mock_requests_get.return_value = mock_response

        kwargs = {'execution_date': datetime(2024, 7, 1), 'ti': MagicMock()}
        obtener_reservas(**kwargs)

        mock_requests_get.assert_called_once()
        mock_makedirs.assert_called_once_with('/tmp', exist_ok=True)
        mock_to_csv.assert_called_once_with('/tmp/reservas_2024-07-01_2024-07-01.csv', index=False)
        kwargs['ti'].xcom_push.assert_called_once_with(key='reservas_file_path', value='/tmp/reservas_2024-07-01_2024-07-01.csv')

    @patch('dags.etl.api_request.requests.get')
    @patch('dags.etl.api_request.os.makedirs')
    @patch('dags.etl.api_request.pd.DataFrame.to_csv')
    def test_obtener_dolar(self, mock_to_csv, mock_makedirs, mock_requests_get):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'fecha': '2024-07-01', 'venta': 150}
        mock_requests_get.return_value = mock_response

        kwargs = {'execution_date': datetime(2024, 7, 1), 'ti': MagicMock()}
        obtener_dolar(**kwargs)

        mock_requests_get.assert_called_once()
        mock_makedirs.assert_called_once_with('/tmp/dolar_2024/07', exist_ok=True)
        mock_to_csv.assert_called_once_with('/tmp/dolar_2024/07/01.csv', index=False)
        kwargs['ti'].xcom_push.assert_called_once_with(key='dolar_file_path', value='/tmp/dolar_2024/07/01.csv')


if __name__ == '__main__':
    unittest.main()