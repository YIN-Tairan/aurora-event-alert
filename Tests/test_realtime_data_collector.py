"""
Tests for realtime_data_collector.py

Covers:
  - crop_txt_header    – strips comment/header lines from raw NOAA text
  - extract_txt_table  – parses valid rows into a DataFrame
  - load_text_file     – downloads plain-text data (mocked HTTP)
  - load_json_file     – downloads JSON data (mocked HTTP)
  - init_ovation_db    – creates the SQLite table for ovation snapshots
  - collect_ovation_aurora – fetches ovation data and stores it (mocked HTTP)
"""

import json
import os
import sys
import sqlite3
import tempfile
import unittest
from unittest.mock import MagicMock, patch

# Allow importing modules from the project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import realtime_data_collector as rdc


# ---------------------------------------------------------------------------
# crop_txt_header
# ---------------------------------------------------------------------------

class TestCropTxtHeader(unittest.TestCase):

    def test_removes_hash_comment_lines(self):
        sample = "# comment line\ndata line 1\ndata line 2\n"
        result = rdc.crop_txt_header(sample)
        self.assertNotIn("# comment line", result)
        self.assertIn("data line 1", result)
        self.assertIn("data line 2", result)

    def test_removes_colon_header_lines(self):
        sample = ": header\n: another header\nactual data\n"
        result = rdc.crop_txt_header(sample)
        for line in result:
            self.assertFalse(line.startswith(":"))

    def test_keeps_empty_lines(self):
        sample = "# skip\n\ndata\n"
        result = rdc.crop_txt_header(sample)
        self.assertIn("data", result)

    def test_all_comments_returns_mostly_empty(self):
        sample = "# only comments\n: here too\n"
        result = rdc.crop_txt_header(sample)
        data_lines = [l for l in result if l.strip()]
        self.assertEqual(data_lines, [])


# ---------------------------------------------------------------------------
# extract_txt_table
# ---------------------------------------------------------------------------

class TestExtractTxtTable(unittest.TestCase):

    COLUMNS = ["Year", "Month", "Day", "Time", "MJD", "SoD", "Status", "PD", "Speed", "Temp"]

    def test_extracts_valid_rows(self):
        lines = [
            "",
            "2024 01 01 0000 59952 0 1 1.5 400 100000",
            "2024 01 01 0001 59952 60 1 1.6 401 100001",
        ]
        df = rdc.extract_txt_table(lines, self.COLUMNS)
        self.assertEqual(len(df), 2)
        self.assertListEqual(list(df.columns), self.COLUMNS)

    def test_skips_rows_with_wrong_column_count(self):
        lines = ["2024 01 01", "2024 01 01 0000 59952 0 1 1.5 400 100000"]
        df = rdc.extract_txt_table(lines, self.COLUMNS)
        self.assertEqual(len(df), 1)

    def test_empty_input_returns_empty_dataframe(self):
        df = rdc.extract_txt_table([], self.COLUMNS)
        self.assertEqual(len(df), 0)


# ---------------------------------------------------------------------------
# load_text_file
# ---------------------------------------------------------------------------

class TestLoadTextFile(unittest.TestCase):

    @patch("realtime_data_collector.requests.get")
    def test_returns_response_text(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.text = "noaa raw text"
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        result = rdc.load_text_file("https://example.com/data.txt")
        self.assertEqual(result, "noaa raw text")

    @patch("realtime_data_collector.requests.get")
    def test_raises_on_http_error(self, mock_get):
        import requests
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = requests.HTTPError("404")
        mock_get.return_value = mock_resp

        with self.assertRaises(Exception):
            rdc.load_text_file("https://example.com/missing.txt")


# ---------------------------------------------------------------------------
# load_json_file
# ---------------------------------------------------------------------------

class TestLoadJsonFile(unittest.TestCase):

    @patch("realtime_data_collector.requests.get")
    def test_returns_parsed_json(self, mock_get):
        payload = {"Observation Time": "2024-01-01T00:00:00Z", "coordinates": []}
        mock_resp = MagicMock()
        mock_resp.text = json.dumps(payload)
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        result = rdc.load_json_file("https://example.com/data.json")
        self.assertEqual(result, payload)


# ---------------------------------------------------------------------------
# init_ovation_db
# ---------------------------------------------------------------------------

class TestInitOvationDb(unittest.TestCase):

    def test_creates_table(self):
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            rdc.init_ovation_db(db_path)
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            conn.close()
            self.assertIn("ovation_aurora_snapshots", tables)
        finally:
            os.unlink(db_path)

    def test_idempotent(self):
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            rdc.init_ovation_db(db_path)
            rdc.init_ovation_db(db_path)  # calling twice should not raise
        finally:
            os.unlink(db_path)


# ---------------------------------------------------------------------------
# collect_ovation_aurora
# ---------------------------------------------------------------------------

class TestCollectOvationAurora(unittest.TestCase):

    @patch("realtime_data_collector.requests.get")
    def test_stores_snapshot(self, mock_get):
        from datetime import datetime, timezone
        # Use a current timestamp so the cleanup DELETE (>1 hour old) does not remove it
        now_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        payload = {
            "Observation Time": now_ts,
            "Forecast Time": now_ts,
            "coordinates": [[0, 0, 0]],
        }
        mock_resp = MagicMock()
        mock_resp.text = json.dumps(payload)
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            rdc.collect_ovation_aurora(db_path)
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT observation_time FROM ovation_aurora_snapshots")
            rows = cursor.fetchall()
            conn.close()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0][0], now_ts)
        finally:
            os.unlink(db_path)


if __name__ == "__main__":
    unittest.main()
