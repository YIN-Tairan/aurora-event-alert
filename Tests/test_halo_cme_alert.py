"""
Tests for halo_cme_alert.py

Covers:
  - get_cme_alerts   – extracts CME events from HTML content
  - is_today_event   – checks whether a t0 timestamp is today (UTC)
  - fetch_html       – downloads the CACTUS page (mocked HTTP)
  - is_already_sent  – deduplication check against the SQLite database
"""

import os
import sys
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

# Allow importing modules from the project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# halo_cme_alert loads SMTP password at module level; satisfy via env var
os.environ.setdefault("SMTP_PASSWORD", "test_password")

import halo_cme_alert as hca


# ---------------------------------------------------------------------------
# get_cme_alerts
# ---------------------------------------------------------------------------

# Minimal HTML that matches what get_cme_alerts expects:
# an <h2 name="Latest"> followed by a <pre> block containing CME timestamps.
_SAMPLE_HTML = """
<html><body>
<h2 name="Latest">Latest events</h2>
<pre>
CME event t0=2025-01-21T09:24:07.532 speed=800 width=HALO
CME event t0=2025-01-22T14:00:00.000 speed=650 width=HALO
</pre>
</body></html>
"""

_EMPTY_HTML = """
<html><body>
<h2 name="Latest">Latest events</h2>
<pre>No events today.</pre>
</body></html>
"""


class TestGetCmeAlerts(unittest.TestCase):

    def test_extracts_timestamps(self):
        events = hca.get_cme_alerts(_SAMPLE_HTML)
        t0_values = [e["t0"] for e in events]
        self.assertIn("2025-01-21T09:24:07.532", t0_values)
        self.assertIn("2025-01-22T14:00:00.000", t0_values)

    def test_returns_full_text_in_each_event(self):
        events = hca.get_cme_alerts(_SAMPLE_HTML)
        for event in events:
            self.assertIn("full_text", event)
            self.assertIsInstance(event["full_text"], str)

    def test_no_timestamps_returns_empty_list(self):
        events = hca.get_cme_alerts(_EMPTY_HTML)
        self.assertEqual(events, [])

    def test_missing_latest_header_returns_empty(self):
        html = "<html><body><h2>Other header</h2><pre>data</pre></body></html>"
        events = hca.get_cme_alerts(html)
        self.assertEqual(events, [])

    def test_missing_pre_block_returns_empty(self):
        html = '<html><body><h2 name="Latest">Latest</h2></body></html>'
        events = hca.get_cme_alerts(html)
        self.assertEqual(events, [])


# ---------------------------------------------------------------------------
# is_today_event
# ---------------------------------------------------------------------------

class TestIsTodayEvent(unittest.TestCase):

    def test_today_returns_true(self):
        today_t0 = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000")
        self.assertTrue(hca.is_today_event(today_t0))

    def test_past_date_returns_false(self):
        self.assertFalse(hca.is_today_event("2000-01-01T00:00:00.000"))

    def test_invalid_format_returns_false(self):
        self.assertFalse(hca.is_today_event("not-a-timestamp"))


# ---------------------------------------------------------------------------
# fetch_html
# ---------------------------------------------------------------------------

class TestFetchHtml(unittest.TestCase):

    @patch("halo_cme_alert.requests.get")
    def test_returns_html_text(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.text = "<html>page</html>"
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        result = hca.fetch_html("https://example.com/cactus/")
        self.assertEqual(result, "<html>page</html>")

    @patch("halo_cme_alert.requests.get")
    def test_returns_none_on_connection_error(self, mock_get):
        import requests as req
        mock_get.side_effect = req.exceptions.RequestException("timeout")

        result = hca.fetch_html("https://example.com/cactus/")
        self.assertIsNone(result)


# ---------------------------------------------------------------------------
# is_already_sent  (uses the module-level global cursor — tested in isolation
#                   via a fresh in-memory DB swapped in for the test)
# ---------------------------------------------------------------------------

class TestIsAlreadySent(unittest.TestCase):

    def setUp(self):
        """Replace the module-level connection and cursor with an in-memory DB."""
        self._orig_conn = hca.conn
        self._orig_cursor = hca.cursor

        self.test_conn = sqlite3.connect(":memory:")
        self.test_cursor = self.test_conn.cursor()
        self.test_cursor.execute(
            "CREATE TABLE sent_alerts (t0 TEXT PRIMARY KEY, event_date TEXT)"
        )
        self.test_conn.commit()

        hca.conn = self.test_conn
        hca.cursor = self.test_cursor

    def tearDown(self):
        self.test_conn.close()
        hca.conn = self._orig_conn
        hca.cursor = self._orig_cursor

    def test_new_event_not_already_sent(self):
        self.assertFalse(hca.is_already_sent("2025-01-21T09:24:07.532"))

    def test_inserted_event_is_already_sent(self):
        t0 = "2025-01-21T09:24:07.532"
        self.test_cursor.execute(
            "INSERT INTO sent_alerts (t0, event_date) VALUES (?, ?)",
            (t0, "2025-01-21"),
        )
        self.test_conn.commit()
        self.assertTrue(hca.is_already_sent(t0))

    def test_different_t0_not_already_sent(self):
        t0a = "2025-01-21T09:24:07.532"
        t0b = "2025-01-22T14:00:00.000"
        self.test_cursor.execute(
            "INSERT INTO sent_alerts (t0, event_date) VALUES (?, ?)",
            (t0a, "2025-01-21"),
        )
        self.test_conn.commit()
        self.assertFalse(hca.is_already_sent(t0b))


if __name__ == "__main__":
    unittest.main()
