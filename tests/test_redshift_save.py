import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from dags.etl.redshift_save import guardar_en_redshift

class TestRedshiftSave(unittest.TestCase):

    @patch('dags.etl.redshift_save.psycopg2.connect')
    def test_guardar_en_redshift(self, mock_connect):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        df = pd.DataFrame({'fecha': ['2024-07-01'], 'col1': ['value1'], 'col2': ['value2']})

        guardar_en_redshift(df, 'test_table')

        mock_connect.assert_called_once()

        expected_delete_query = "DELETE FROM test_table WHERE fecha IN ('2024-07-01');"
        mock_cursor.execute.assert_any_call(expected_delete_query)

        expected_insert_query = (
            "\n        INSERT INTO test_table (fecha, col1, col2)\n        VALUES (%s, %s, %s);\n        "
        )
        mock_cursor.execute.assert_any_call(expected_insert_query, ('2024-07-01', 'value1', 'value2'))

        mock_conn.commit.assert_called_once()
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()

if __name__ == '__main__':
    unittest.main()
