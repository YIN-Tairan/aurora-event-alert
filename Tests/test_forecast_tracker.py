"""
Tests for forecast_tracker.py

Covers:
  - parse_and_complete_date       – 'Jan 25' → 'yyyy-mm-dd' with year inference
  - replace_date_with_annotation  – annotates dates of interest in text
  - previous_day                  – returns the day before a given date (or None if past)
  - get_kp_forecast               – parses 3-day KP forecast from NOAA (mocked HTTP)
"""

import os
import sys
import unittest
from datetime import datetime, date
from unittest.mock import MagicMock, patch

# Allow importing modules from the project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# forecast_tracker imports 'travel' (which reads keypwd.json at module level)
# and 'weather_query'; mock both before importing the module.
_mock_travel = MagicMock()
_mock_wq = MagicMock()
sys.modules.setdefault("travel", _mock_travel)
sys.modules.setdefault("weather_query", _mock_wq)

# Also ensure SMTP_PASSWORD is set so MailInfo doesn't need keypwd.json
os.environ.setdefault("SMTP_PASSWORD", "test_password")

import forecast_tracker as ft


# ---------------------------------------------------------------------------
# parse_and_complete_date
# ---------------------------------------------------------------------------

class TestParseAndCompleteDate(unittest.TestCase):

    def test_basic_parse(self):
        ref = datetime(2025, 6, 15)
        result = ft.parse_and_complete_date("Jun 10", current_date=ref)
        self.assertEqual(result, "2025-06-10")

    def test_year_rollover_december_to_january(self):
        # In December, a January date should roll to the next year
        ref = datetime(2025, 12, 20)
        result = ft.parse_and_complete_date("Jan 05", current_date=ref)
        self.assertEqual(result, "2026-01-05")

    def test_no_rollover_in_other_months(self):
        ref = datetime(2025, 3, 1)
        result = ft.parse_and_complete_date("Feb 14", current_date=ref)
        self.assertEqual(result, "2025-02-14")

    def test_invalid_format_raises(self):
        with self.assertRaises(ValueError):
            ft.parse_and_complete_date("2025-01-01")

    def test_uses_current_date_when_none(self):
        # Should not raise and should return a valid date string
        result = ft.parse_and_complete_date("Jan 01")
        self.assertRegex(result, r"^\d{4}-01-01$")


# ---------------------------------------------------------------------------
# replace_date_with_annotation
# ---------------------------------------------------------------------------

class TestReplaceDateWithAnnotation(unittest.TestCase):

    def test_replaces_target_date(self):
        text = "Event on 2025-05-10 looks interesting."
        result = ft.replace_date_with_annotation(text, "2025-05-10")
        self.assertIn("2025-05-10(DATE OF KP)", result)

    def test_does_not_alter_other_dates(self):
        text = "Dates: 2025-05-10 and 2025-06-15"
        result = ft.replace_date_with_annotation(text, "2025-05-10")
        self.assertIn("2025-06-15", result)
        self.assertNotIn("2025-06-15(DATE OF KP)", result)

    def test_returns_unchanged_when_no_match(self):
        text = "No interesting date here."
        result = ft.replace_date_with_annotation(text, "2025-05-10")
        self.assertEqual(result, text)


# ---------------------------------------------------------------------------
# previous_day
# ---------------------------------------------------------------------------

class TestPreviousDay(unittest.TestCase):

    def test_future_date_returns_day_before(self):
        from datetime import timezone, timedelta
        future = (datetime.now(timezone.utc).date() + timedelta(days=10)).strftime("%Y-%m-%d")
        result = ft.previous_day(future)
        from datetime import timedelta as td
        expected = (datetime.strptime(future, "%Y-%m-%d").date() - td(days=1)).strftime("%Y-%m-%d")
        self.assertEqual(result, expected)

    def test_past_date_returns_none(self):
        past = "2000-01-01"
        result = ft.previous_day(past)
        self.assertIsNone(result)


# ---------------------------------------------------------------------------
# get_kp_forecast  (mocked NOAA response)
# ---------------------------------------------------------------------------

# Minimal 3-day-forecast.txt content that get_kp_forecast can parse
_SAMPLE_FORECAST = """\
:Product: 3-Day Forecast
:
# Begin 3-Day Forecast

A. NOAA Geomagnetic Activity Observation and Forecast

The greatest observed 3 hr Kp over the past 24 hours was 2 (below NOAA
Scale levels).
The greatest expected 3 hr Kp over the next 24 hours is 2 (below NOAA
Scale levels).

NOAA Kp index breakdown Apr 07-Apr 09, 2025
             Apr 07       Apr 08       Apr 09

00-03UT      2            2            2
03-06UT      2            2            2
06-09UT      2            2            2
09-12UT      2            2            2
12-15UT      2            2            2
15-18UT      2            2            2
18-21UT      2            2            2
21-00UT      2            2            2

B. NOAA Solar Radiation Activity Observation and Forecast
"""


class TestGetKpForecast(unittest.TestCase):

    @patch("forecast_tracker.requests.get")
    def test_returns_correct_structure(self, mock_get):
        import tempfile, os
        # get_kp_forecast writes a CSV to ./data/ — create the directory
        os.makedirs("data", exist_ok=True)

        mock_resp = MagicMock()
        mock_resp.text = _SAMPLE_FORECAST
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        result = ft.get_kp_forecast()
        # result is [kp5_bool, kp7_bool, response_text, interesting_dates]
        self.assertEqual(len(result), 4)
        kp5, kp7, text, dates = result
        self.assertIsInstance(kp5, bool)
        self.assertIsInstance(kp7, bool)
        self.assertIsInstance(text, str)
        self.assertIsInstance(dates, list)

    @patch("forecast_tracker.requests.get")
    def test_detects_kp5(self, mock_get):
        os.makedirs("data", exist_ok=True)
        forecast_kp5 = _SAMPLE_FORECAST.replace(
            "00-03UT      2            2            2",
            "00-03UT      5            5            5",
        )
        mock_resp = MagicMock()
        mock_resp.text = forecast_kp5
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        kp5, kp7, _, _ = ft.get_kp_forecast()
        self.assertTrue(kp5)
        self.assertFalse(kp7)

    @patch("forecast_tracker.requests.get")
    def test_detects_kp7(self, mock_get):
        os.makedirs("data", exist_ok=True)
        forecast_kp7 = _SAMPLE_FORECAST.replace(
            "00-03UT      2            2            2",
            "00-03UT      7            7            7",
        )
        mock_resp = MagicMock()
        mock_resp.text = forecast_kp7
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        kp5, kp7, _, _ = ft.get_kp_forecast()
        self.assertTrue(kp5)
        self.assertTrue(kp7)

    @patch("forecast_tracker.requests.get")
    def test_no_kp5_when_all_low(self, mock_get):
        os.makedirs("data", exist_ok=True)
        mock_resp = MagicMock()
        mock_resp.text = _SAMPLE_FORECAST
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        kp5, kp7, _, _ = ft.get_kp_forecast()
        self.assertFalse(kp5)
        self.assertFalse(kp7)


if __name__ == "__main__":
    unittest.main()
