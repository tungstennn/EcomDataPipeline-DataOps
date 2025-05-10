from unittest.mock import MagicMock, mock_open, patch
from scripts.copy_to_redshift import copy_data

@patch('builtins.open', new_callable=mock_open, read_data='COPY some_table FROM ...;')
def test_copy_data_success(mock_file):
    # Mock connection and cursor
    mock_conn = MagicMock()
    mock_cur = MagicMock()

    # Run the function
    copy_data(mock_conn, mock_cur)

    # Check that the file was read and SQL was executed
    mock_file.assert_called_once_with('sql/copy_to_redshift.sql', 'r')
    mock_cur.execute.assert_called_once_with('COPY some_table FROM ...;')
    mock_conn.rollback.assert_not_called()

@patch('builtins.open', new_callable=mock_open, read_data='COPY some_table FROM ...;')
def test_copy_data_failure(mock_file):
    # Mock connection and cursor
    mock_conn = MagicMock()
    mock_cur = MagicMock()

    # Simulate an exception during SQL execution
    mock_cur.execute.side_effect = Exception("Test SQL error")

    # Run the function
    copy_data(mock_conn, mock_cur)

    # Should trigger rollback on exception
    mock_conn.rollback.assert_called_once()
