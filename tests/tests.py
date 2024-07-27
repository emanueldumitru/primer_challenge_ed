import unittest
import sys
import os
import sqlite3
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))
from main import extract_wal_records_mapping, process_wal_records_mapping, persist_metrics_data


class TestMain(unittest.TestCase):

    def setUp(self):
        self.test_wal_path = "tests/test_wal.json"
        self.test_db_path = "tests/test.db"

        if os.path.exists(self.test_db_path):
            os.remove(self.test_db_path)

    def test_extract_wal_records_mapping(self):
        result = extract_wal_records_mapping(self.test_wal_path)

        self.assertIn('event_v2_data', result)
        self.assertIn('transaction', result)
        self.assertIn('transaction_request', result)
        self.assertIn('payment_instrument_token_data', result)

        self.assertEqual(len(result['event_v2_data']), 576)
        self.assertEqual(len(result['transaction']), 140)
        self.assertEqual(len(result['transaction_request']), 140)
        self.assertEqual(len(result['payment_instrument_token_data']), 144)

    def test_process_wal_records_mapping(self):
        result = extract_wal_records_mapping(self.test_wal_path)
        metrics_mapping = process_wal_records_mapping(result)

        self.assertIn('types', metrics_mapping)
        self.assertIn('data', metrics_mapping)
        self.assertEqual(len(metrics_mapping['types']), 15)
        self.assertEqual(len(metrics_mapping['data']), 529)

    def test_persist_metrics_data(self):
        result = extract_wal_records_mapping(self.test_wal_path)
        metrics_mapping = process_wal_records_mapping(result)
        persist_metrics_data(metrics_mapping, db_file_path = self.test_db_path)

        conection = sqlite3.connect(self.test_db_path)
        self.assertIsNot(conection, 0)

        cursor = conection.cursor()
        cursor.execute("select count(*) from metrics")
        records_count = cursor.fetchone()[0]

        cursor.close()

        self.assertEqual(records_count, 529)


if __name__ == '__main__':
    unittest.main()
