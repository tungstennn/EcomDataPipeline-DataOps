from unittest.mock import MagicMock, mock_open, patch
from scripts.copy_to_redshift import copy_data

def test_copy_data_success():
    # --- Arrange ---
    mock_conn = MagicMock()
    mock_cur = MagicMock()

    fake_sql = 'COPY some_table FROM ...;'
    m = mock_open(read_data=fake_sql)

    # --- Act ---
    with patch('builtins.open', m):
        copy_data(mock_conn, mock_cur)

    # --- Assert ---
    mock_cur.execute.assert_called_once_with(fake_sql)
    mock_conn.rollback.assert_not_called()

def test_copy_data_failure():
    # --- Arrange ---
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_cur.execute.side_effect = Exception("Test SQL error")

    m = mock_open(read_data='COPY some_table FROM ...;')

    # --- Act ---
    with patch('builtins.open', m):
        copy_data(mock_conn, mock_cur)

    # --- Assert ---
    mock_conn.rollback.assert_called_once()
