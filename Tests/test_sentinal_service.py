"""
Tests for sentinal_service.py

Covers:
  - compute_geomagnetic_latitude – dipole-based latitude conversion
  - _safe_floats                 – NOAA sentinel value filtering
  - _linear_trend                – regression slope calculation
  - estimate_kp_from_conditions  – empirical Kp estimation
  - _kp_to_aurora_probability    – sigmoid probability conversion
  - predict_aurora               – end-to-end prediction using a temp DB
"""

import os
import sys
import math
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone, timedelta

# Allow importing modules from the project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import sentinal_service as ss


# ---------------------------------------------------------------------------
# compute_geomagnetic_latitude
# ---------------------------------------------------------------------------

class TestComputeGeomagneticLatitude(unittest.TestCase):

    def test_returns_float(self):
        result = ss.compute_geomagnetic_latitude(65.0, 25.0)
        self.assertIsInstance(result, float)

    def test_high_latitude_stays_high(self):
        # Finnish Lapland (~68°N) should map to a geomagnetic latitude > 60°
        result = ss.compute_geomagnetic_latitude(68.0, 27.0)
        self.assertGreater(result, 60.0)

    def test_equator_gives_moderate_lat(self):
        result = ss.compute_geomagnetic_latitude(0.0, 0.0)
        self.assertLess(abs(result), 30.0)

    def test_result_in_valid_range(self):
        for lat, lon in [(90, 0), (-90, 0), (0, 180), (45, -90)]:
            result = ss.compute_geomagnetic_latitude(lat, lon)
            self.assertGreaterEqual(result, -90.0)
            self.assertLessEqual(result, 90.0)


# ---------------------------------------------------------------------------
# _safe_floats
# ---------------------------------------------------------------------------

class TestSafeFloats(unittest.TestCase):

    def test_filters_noaa_sentinel_minus9999(self):
        rows = [{"bz": -9999.0}, {"bz": -5.0}]
        self.assertEqual(ss._safe_floats(rows, "bz"), [-5.0])

    def test_filters_noaa_sentinel_minus999(self):
        rows = [{"bz": -999.0}, {"bz": 2.5}]
        self.assertEqual(ss._safe_floats(rows, "bz"), [2.5])

    def test_filters_none_values(self):
        rows = [{"bz": None}, {"bz": 3.0}]
        self.assertEqual(ss._safe_floats(rows, "bz"), [3.0])

    def test_filters_non_numeric_strings(self):
        rows = [{"bz": "n/a"}, {"bz": "1.5"}]
        self.assertEqual(ss._safe_floats(rows, "bz"), [1.5])

    def test_missing_key_returns_empty(self):
        rows = [{"speed": 400.0}]
        self.assertEqual(ss._safe_floats(rows, "bz"), [])

    def test_all_valid_values_preserved(self):
        rows = [{"bz": v} for v in [-10.0, 0.0, 5.0]]
        self.assertEqual(ss._safe_floats(rows, "bz"), [-10.0, 0.0, 5.0])


# ---------------------------------------------------------------------------
# _linear_trend
# ---------------------------------------------------------------------------

class TestLinearTrend(unittest.TestCase):

    def test_increasing_sequence(self):
        slope = ss._linear_trend([1.0, 2.0, 3.0, 4.0, 5.0])
        self.assertAlmostEqual(slope, 1.0, places=5)

    def test_decreasing_sequence(self):
        slope = ss._linear_trend([5.0, 4.0, 3.0, 2.0, 1.0])
        self.assertAlmostEqual(slope, -1.0, places=5)

    def test_flat_sequence(self):
        self.assertAlmostEqual(ss._linear_trend([7.0, 7.0, 7.0]), 0.0, places=5)

    def test_single_value_returns_zero(self):
        self.assertEqual(ss._linear_trend([3.0]), 0.0)

    def test_empty_returns_zero(self):
        self.assertEqual(ss._linear_trend([]), 0.0)


# ---------------------------------------------------------------------------
# estimate_kp_from_conditions
# ---------------------------------------------------------------------------

class TestEstimateKpFromConditions(unittest.TestCase):

    def test_quiet_conditions_give_low_kp(self):
        # Positive (northward) Bz, low speed, zero NHPI → all contributions are 0
        kp = ss.estimate_kp_from_conditions(bz=2.0, bulk_speed=350.0, nhpi=0.0)
        self.assertAlmostEqual(kp, 0.0, places=5)

    def test_strongly_southward_bz_raises_kp(self):
        kp = ss.estimate_kp_from_conditions(bz=-15.0, bulk_speed=400.0, nhpi=0.0)
        self.assertGreater(kp, 4.0)

    def test_high_speed_raises_kp(self):
        kp = ss.estimate_kp_from_conditions(bz=0.0, bulk_speed=900.0, nhpi=0.0)
        self.assertGreater(kp, 0.0)

    def test_result_clamped_to_0_9(self):
        kp = ss.estimate_kp_from_conditions(bz=-100.0, bulk_speed=3000.0, nhpi=5000.0)
        self.assertLessEqual(kp, 9.0)
        self.assertGreaterEqual(kp, 0.0)

    def test_none_inputs_return_zero(self):
        kp = ss.estimate_kp_from_conditions(bz=None, bulk_speed=None, nhpi=None)
        self.assertAlmostEqual(kp, 0.0, places=5)


# ---------------------------------------------------------------------------
# _kp_to_aurora_probability
# ---------------------------------------------------------------------------

class TestKpToAuroraProbability(unittest.TestCase):

    def test_probability_in_range(self):
        for kp in [0, 3, 6, 9]:
            prob = ss._kp_to_aurora_probability(kp, observer_geomag_lat=65.0)
            self.assertGreaterEqual(prob, 0.0)
            self.assertLessEqual(prob, 1.0)

    def test_high_kp_raises_probability_at_mid_lat(self):
        low_kp_prob = ss._kp_to_aurora_probability(1, observer_geomag_lat=55.0)
        high_kp_prob = ss._kp_to_aurora_probability(8, observer_geomag_lat=55.0)
        self.assertGreater(high_kp_prob, low_kp_prob)

    def test_high_lat_observer_has_high_prob(self):
        prob = ss._kp_to_aurora_probability(5, observer_geomag_lat=75.0)
        self.assertGreater(prob, 0.7)

    def test_low_lat_observer_has_low_prob_quiet(self):
        prob = ss._kp_to_aurora_probability(1, observer_geomag_lat=30.0)
        self.assertLess(prob, 0.2)


# ---------------------------------------------------------------------------
# predict_aurora (integration with a temporary SQLite database)
# ---------------------------------------------------------------------------

def _make_test_db(n_rows=15, bz=-5.0, speed=450.0, nhpi=30.0, kp=3.0):
    """Create a temporary aurora_data database with synthetic rows."""
    f = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    db_path = f.name
    f.close()

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE aurora_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            datetime TEXT,
            modified_julian_day TEXT,
            seconds_of_day TEXT,
            status TEXT,
            proton_density REAL,
            bulk_speed REAL,
            ion_temperature REAL,
            bx REAL, by REAL, bz REAL, bt REAL,
            latitude REAL, longitude REAL,
            forecast TEXT,
            north_hemi_power_index REAL,
            south_hemi_power_index REAL,
            realtime_kp REAL
        )
    """)
    now = datetime.now(timezone.utc)
    for i in range(n_rows):
        t = now - timedelta(minutes=i)
        cursor.execute(
            "INSERT INTO aurora_data "
            "(datetime, bz, bulk_speed, north_hemi_power_index, south_hemi_power_index, realtime_kp) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (t.isoformat(" "), bz, speed, nhpi, nhpi * 0.9, kp),
        )
    conn.commit()
    conn.close()
    return db_path


class TestPredictAurora(unittest.TestCase):

    def setUp(self):
        self.db_path = _make_test_db()

    def tearDown(self):
        os.unlink(self.db_path)

    def test_returns_predictions_dict(self):
        result = ss.predict_aurora(65.0, 25.0, db_path=self.db_path)
        self.assertNotIn("error", result)
        self.assertIn("predictions", result)
        for horizon in ("5min", "30min", "1hr"):
            self.assertIn(horizon, result["predictions"])

    def test_prediction_probabilities_in_range(self):
        result = ss.predict_aurora(65.0, 25.0, db_path=self.db_path)
        for horizon in ("5min", "30min", "1hr"):
            prob = result["predictions"][horizon]["aurora_probability"]
            self.assertGreaterEqual(prob, 0.0)
            self.assertLessEqual(prob, 1.0)

    def test_empty_db_returns_error(self):
        empty_path = _make_test_db(n_rows=0)
        try:
            result = ss.predict_aurora(65.0, 25.0, db_path=empty_path)
            self.assertIn("error", result)
        finally:
            os.unlink(empty_path)

    def test_storm_conditions_give_higher_probability(self):
        quiet_path = _make_test_db(bz=2.0, speed=350.0, nhpi=5.0, kp=0.5)
        storm_path = _make_test_db(bz=-20.0, speed=800.0, nhpi=200.0, kp=8.0)
        try:
            quiet = ss.predict_aurora(55.0, 25.0, db_path=quiet_path)
            storm = ss.predict_aurora(55.0, 25.0, db_path=storm_path)
            self.assertGreater(
                storm["predictions"]["5min"]["aurora_probability"],
                quiet["predictions"]["5min"]["aurora_probability"],
            )
        finally:
            os.unlink(quiet_path)
            os.unlink(storm_path)


if __name__ == "__main__":
    unittest.main()
