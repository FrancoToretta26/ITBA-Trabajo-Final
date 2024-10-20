from datetime import datetime
import unittest
from unittest.mock import patch, MagicMock
from dags.etl.api_request import obtener_reservas, obtener_dolar

class TestApiRequest(unittest.TestCase):

    @patch('dags.etl.api_request.requests.get')
    @patch('dags.etl.api_request.guardar_en_redshift')
    def test_obtener_reservas(self, mock_guardar_en_redshift, mock_requests_get):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'results': [{'fecha': '2024-07-01', 'valor': 100}]}
        mock_requests_get.return_value = mock_response

        kwargs = {'execution_date': datetime(2024, 7, 1), 'ti': MagicMock()}
        obtener_reservas(**kwargs)

        mock_requests_get.assert_called_once()
        mock_guardar_en_redshift.assert_called_once()

    @patch('dags.etl.api_request.requests.get')
    @patch('dags.etl.api_request.guardar_en_redshift')
    def test_obtener_dolar(self, mock_guardar_en_redshift, mock_requests_get):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'fecha': '2024-07-01', 'venta': 150}
        mock_requests_get.return_value = mock_response

        kwargs = {'execution_date': datetime(2024, 7, 1), 'ti': MagicMock()}
        obtener_dolar(**kwargs)

        mock_requests_get.assert_called_once()
        mock_guardar_en_redshift.assert_called_once()

if __name__ == '__main__':
    unittest.main()