import sqlite3
import argparse
import time
import math
import statistics
from datetime import datetime, timedelta
import pytz

# Approximate IGRF geomagnetic north pole position
_POLE_LAT_RAD = math.radians(80.65)
_POLE_LON_RAD = math.radians(-72.68)

# Prediction time horizons (minutes / data points at 1-min resolution)
HORIZON_5MIN  = 5
HORIZON_30MIN = 30
HORIZON_1HR   = 60

# Physical clamping bounds for extrapolated values
BZ_MIN    = -50.0   # nT
BZ_MAX    =  50.0   # nT
SPEED_MIN =  200.0  # km/s
SPEED_MAX = 1500.0  # km/s
NHPI_MIN  =    0.0  # GW
NHPI_MAX  = 1000.0  # GW

# Kp estimation weights (must sum to 1.0)
# Based on empirical literature: Bz dominates (~60 %), speed and NHPI secondary
KP_BZ_WEIGHT    = 0.6
KP_SPEED_WEIGHT = 0.2
KP_NHPI_WEIGHT  = 0.2

# Empirical coefficients for Kp sub-scores
BZ_TO_KP_COEFF    = 0.6   # maps |Bz| (nT) → Kp contribution; -15 nT ≈ Kp 9
SPEED_KP_SCALE    = 100.0 # km/s above 400 per Kp unit
NHPI_KP_SCALE     = 20.0  # GW per Kp unit

# Aurora visibility: auroral oval latitude model
# Minimum visible geomagnetic latitude ≈ BASE_LAT − Kp × KP_LAT_COEFF
BASE_GEOMAG_LAT  = 67.0   # degrees — quiet-time lower edge of auroral oval
KP_LAT_COEFF     =  3.0   # degrees latitude shift per Kp unit
MIN_GEOMAG_LAT   = 40.0   # hard floor (auroral oval never reaches lower latitudes)

# Sigmoid steepness for probability conversion (degrees of latitude)
SIGMOID_STEEPNESS = 5.0

# Blending weights for Kp prediction at each horizon
# (estimated_kp_weight, realtime_kp_weight)
KP_5MIN_BLEND    = (0.7, 0.3)  # short horizon → trust estimates, validate with realtime
KP_30MIN_BLEND   = (0.6, 0.4)  # medium horizon → moderate trust in realtime
KP_1HR_BLEND     = (0.5, 0.5)  # long horizon → equal weight


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def latest_hour_data(db_path="aurora_data.db", timezone="UTC"):
    """
    Query aurora_data table for the most recent hour of records.

    Args:
        db_path (str): SQLite database file path.
        timezone (str): Target timezone for datetime conversion (e.g. "UTC" or "Asia/Shanghai").

    Returns:
        List[dict]: Records from the past hour, newest first.
    """
    conn = None
    try:
        utc_now = datetime.now(pytz.utc)
        one_hour_ago = utc_now - timedelta(hours=1)

        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        query = (
            "SELECT * FROM aurora_data "
            "WHERE datetime BETWEEN ? AND ? "
            "ORDER BY datetime DESC"
        )
        cursor.execute(query, (one_hour_ago.isoformat(' '), utc_now.isoformat(' ')))
        data = [dict(row) for row in cursor.fetchall()]

        if timezone.upper() != "UTC":
            target_tz = pytz.timezone(timezone)
            for row in data:
                dt_str = row.get("datetime")
                if dt_str:
                    try:
                        dt_obj = datetime.fromisoformat(dt_str)
                    except Exception:
                        dt_obj = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
                    if dt_obj.tzinfo is None:
                        dt_obj = dt_obj.replace(tzinfo=pytz.utc)
                    row["datetime"] = dt_obj.astimezone(target_tz).isoformat(' ')

        return data

    except Exception as e:
        print("Error:", e)
        return []
    finally:
        if conn:
            conn.close()


def get_recent_data(db_path="aurora_data.db", minutes=30):
    """
    Query aurora_data table for records from the past *minutes* minutes,
    ordered oldest-first (for trend calculations).

    Args:
        db_path (str): SQLite database file path.
        minutes (int): Look-back window in minutes.

    Returns:
        List[dict]: Matching records, oldest first.
    """
    conn = None
    try:
        utc_now = datetime.now(pytz.utc)
        cutoff = utc_now - timedelta(minutes=minutes)

        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute(
            "SELECT * FROM aurora_data WHERE datetime >= ? ORDER BY datetime ASC",
            (cutoff.isoformat(' '),),
        )
        return [dict(r) for r in cursor.fetchall()]

    except Exception as e:
        print(f"[DB Error] {e}")
        return []
    finally:
        if conn:
            conn.close()


# ---------------------------------------------------------------------------
# Legacy interest-check helpers (kept for backward compatibility)
# ---------------------------------------------------------------------------

def realtime_kp_is_interesting(dataline):
    """Return True if the real-time Kp value is >= 5."""
    kp = dataline.get("realtime_kp")
    return kp is not None and kp >= 5


def predicted_nhpi_is_interesting(lines):
    """
    Return [True, line] if any line has a forecast time close to now and NHPI > 70,
    otherwise [False, None].
    """
    now_time = datetime.now(tz=pytz.utc)
    for line in lines:
        try:
            forecast_time = datetime.strptime(line["forecast"], "%Y-%m-%d_%H:%M").replace(
                tzinfo=pytz.utc
            )
            if abs((forecast_time - now_time).total_seconds()) < 120 and line["north_hemi_power_index"] > 70:
                return [True, line]
        except Exception:
            continue
    return [False, None]


def nowcasted_nhpi_is_interesting(dataline):
    """Return True if the nowcasted North Hemispheric Power Index is >= 10 GW."""
    nhpi = dataline.get("north_hemi_power_index")
    return nhpi is not None and nhpi >= 10


# ---------------------------------------------------------------------------
# Geomagnetic latitude
# ---------------------------------------------------------------------------

def compute_geomagnetic_latitude(geo_lat, geo_lon):
    """
    Approximate geomagnetic latitude from geographic coordinates using the IGRF
    dipole pole position (~80.65°N, 72.68°W).

    Args:
        geo_lat (float): Geographic latitude in degrees.
        geo_lon (float): Geographic longitude in degrees.

    Returns:
        float: Approximate geomagnetic latitude in degrees.
    """
    lat_rad = math.radians(geo_lat)
    lon_rad = math.radians(geo_lon)
    sin_mag = (
        math.sin(lat_rad) * math.sin(_POLE_LAT_RAD)
        + math.cos(lat_rad) * math.cos(_POLE_LAT_RAD) * math.cos(lon_rad - _POLE_LON_RAD)
    )
    return math.degrees(math.asin(max(-1.0, min(1.0, sin_mag))))


# ---------------------------------------------------------------------------
# Statistical helpers
# ---------------------------------------------------------------------------

def _safe_floats(rows, field):
    """Extract non-None float values for *field* from a list of row dicts."""
    vals = []
    for r in rows:
        v = r.get(field)
        try:
            vals.append(float(v))
        except (TypeError, ValueError):
            pass
    return vals


def _linear_trend(values):
    """
    Compute the linear-regression slope over a sequence of values.

    The x-axis is the index (0, 1, 2, …), so the slope is expressed as
    *change per data point*.  Positive → increasing, negative → decreasing.

    Returns:
        float: Slope, or 0.0 when fewer than 2 values are provided.
    """
    n = len(values)
    if n < 2:
        return 0.0
    mean_x = (n - 1) / 2.0
    mean_y = sum(values) / n
    num = sum((i - mean_x) * (values[i] - mean_y) for i in range(n))
    den = sum((i - mean_x) ** 2 for i in range(n))
    return num / den if den != 0 else 0.0


# ---------------------------------------------------------------------------
# Kp / probability estimation
# ---------------------------------------------------------------------------

def estimate_kp_from_conditions(bz, bulk_speed, nhpi):
    """
    Empirically estimate the Kp index from instantaneous space-weather conditions.

    Weights:
      - Bz (southward component): 60 %
      - Solar wind bulk speed:    20 %
      - North Hemispheric Power:  20 %

    Args:
        bz (float | None): IMF Bz in nT.
        bulk_speed (float | None): Solar wind bulk speed in km/s.
        nhpi (float | None): North Hemispheric Power Index in GW.

    Returns:
        float: Estimated Kp in [0, 9].
    """
    # Bz contribution — only negative (southward) Bz drives geomagnetic activity
    bz_kp = 0.0
    if bz is not None and bz < 0:
        bz_kp = min(abs(bz) * BZ_TO_KP_COEFF, 9.0)

    # Solar wind speed contribution
    speed_kp = 0.0
    if bulk_speed is not None and bulk_speed > 400:
        speed_kp = min((bulk_speed - 400) / SPEED_KP_SCALE, 3.0)

    # NHPI contribution
    nhpi_kp = 0.0
    if nhpi is not None and nhpi > 0:
        nhpi_kp = min(nhpi / NHPI_KP_SCALE, 3.0)

    return min(max(KP_BZ_WEIGHT * bz_kp + KP_SPEED_WEIGHT * speed_kp + KP_NHPI_WEIGHT * nhpi_kp, 0.0), 9.0)


def _min_geomag_lat_for_kp(kp):
    """Return the minimum geomagnetic latitude (degrees) for aurora visibility at the given Kp."""
    return max(MIN_GEOMAG_LAT, BASE_GEOMAG_LAT - kp * KP_LAT_COEFF)


def _kp_to_aurora_probability(kp, observer_geomag_lat):
    """
    Convert an estimated Kp value and observer geomagnetic latitude to an aurora
    visibility probability using a logistic (sigmoid) function.

    The function is centred on the minimum-visibility latitude for the given Kp;
    observers above that latitude have probability > 0.5.

    Args:
        kp (float): Estimated Kp index.
        observer_geomag_lat (float): Observer's geomagnetic latitude in degrees.

    Returns:
        float: Probability in [0, 1], rounded to 3 decimal places.
    """
    obs_lat = abs(observer_geomag_lat)
    min_lat = _min_geomag_lat_for_kp(kp)
    delta = obs_lat - min_lat  # positive ↔ observer is at or above the auroral oval
    return round(1.0 / (1.0 + math.exp(-delta / SIGMOID_STEEPNESS)), 3)


def _probability_label(prob):
    """Return a human-readable label for a probability value."""
    if prob >= 0.8:
        return "Very High"
    if prob >= 0.6:
        return "High"
    if prob >= 0.4:
        return "Moderate"
    if prob >= 0.2:
        return "Low"
    return "Very Low"


# ---------------------------------------------------------------------------
# Core prediction function
# ---------------------------------------------------------------------------

def predict_aurora(geo_lat, geo_lon, db_path="aurora_data.db"):
    """
    Predict aurora visibility probability at a GPS location for three time horizons:
    5 minutes, 30 minutes, and 1 hour ahead.

    The prediction uses data from the past 30 minutes stored by the real-time
    data collector.  Three modelling strategies are applied:

    * **5 min** — Current conditions extrapolated with the short-term trend.
      The observed real-time Kp (if available) is blended in with high weight.
    * **30 min** — Trend-extrapolated conditions blended with the 30-minute
      average to reduce noise.  Real-time Kp contributes with medium weight.
    * **1 hr**  — The 30-minute average dominates; trend extrapolation contributes
      with low weight to capture directional signals without over-projecting.

    Args:
        geo_lat (float): Observer geographic latitude in degrees.
        geo_lon (float): Observer geographic longitude in degrees.
        db_path (str): SQLite database file path.

    Returns:
        dict: Prediction result containing location info, current conditions,
              trends, and per-horizon probability estimates.
    """
    geomag_lat = compute_geomagnetic_latitude(geo_lat, geo_lon)
    rows = get_recent_data(db_path, minutes=30)

    if not rows:
        return {
            "error": "No recent data available in the database.",
            "geo_lat": geo_lat,
            "geo_lon": geo_lon,
            "geomag_lat": round(geomag_lat, 2),
        }

    # --- Extract time series ---
    bz_vals    = _safe_floats(rows, "bz")
    speed_vals = _safe_floats(rows, "bulk_speed")
    nhpi_vals  = _safe_floats(rows, "north_hemi_power_index")
    shpi_vals  = _safe_floats(rows, "south_hemi_power_index")
    kp_vals    = _safe_floats(rows, "realtime_kp")

    # Current (latest) values
    cur_bz    = bz_vals[-1]    if bz_vals    else None
    cur_speed = speed_vals[-1] if speed_vals else None
    cur_nhpi  = nhpi_vals[-1]  if nhpi_vals  else None
    cur_shpi  = shpi_vals[-1]  if shpi_vals  else None
    cur_kp    = kp_vals[-1]    if kp_vals    else None

    # Linear trend (slope per data point; data is 1-minute resolution, so ≈ per minute)
    bz_trend    = _linear_trend(bz_vals)    if len(bz_vals)    >= 3 else 0.0
    speed_trend = _linear_trend(speed_vals) if len(speed_vals) >= 3 else 0.0
    nhpi_trend  = _linear_trend(nhpi_vals)  if len(nhpi_vals)  >= 3 else 0.0
    shpi_trend  = _linear_trend(shpi_vals)  if len(shpi_vals)  >= 3 else 0.0

    # 30-minute averages
    avg_bz    = statistics.mean(bz_vals)    if bz_vals    else None
    avg_speed = statistics.mean(speed_vals) if speed_vals else None
    avg_nhpi  = statistics.mean(nhpi_vals)  if nhpi_vals  else None

    def _clamp(v, lo, hi):
        return max(lo, min(hi, v)) if v is not None else None

    # ---- 5-minute prediction ----
    # Extrapolate current values by HORIZON_5MIN data points (≈ 5 min)
    bz_5    = _clamp((cur_bz    + bz_trend    * HORIZON_5MIN) if cur_bz    is not None else avg_bz,    BZ_MIN, BZ_MAX)
    spd_5   = _clamp((cur_speed + speed_trend * HORIZON_5MIN) if cur_speed is not None else avg_speed, SPEED_MIN, SPEED_MAX)
    nhpi_5  = _clamp((cur_nhpi  + nhpi_trend  * HORIZON_5MIN) if cur_nhpi  is not None else avg_nhpi,  NHPI_MIN, NHPI_MAX)
    kp_5 = estimate_kp_from_conditions(bz_5, spd_5, nhpi_5)
    if cur_kp is not None:
        kp_5 = KP_5MIN_BLEND[0] * kp_5 + KP_5MIN_BLEND[1] * cur_kp

    # ---- 30-minute prediction ----
    # Extrapolate current values by HORIZON_30MIN data points, then blend with the average
    bz_30   = _clamp((cur_bz    + bz_trend    * HORIZON_30MIN) if cur_bz    is not None else avg_bz,    BZ_MIN, BZ_MAX)
    spd_30  = _clamp((cur_speed + speed_trend * HORIZON_30MIN) if cur_speed is not None else avg_speed, SPEED_MIN, SPEED_MAX)
    nhpi_30 = _clamp((cur_nhpi  + nhpi_trend  * HORIZON_30MIN) if cur_nhpi  is not None else avg_nhpi,  NHPI_MIN, NHPI_MAX)
    kp_30_trend = estimate_kp_from_conditions(bz_30, spd_30, nhpi_30)
    kp_30_avg   = estimate_kp_from_conditions(avg_bz, avg_speed, avg_nhpi)
    kp_30 = 0.5 * kp_30_trend + 0.5 * kp_30_avg
    if cur_kp is not None:
        kp_30 = KP_30MIN_BLEND[0] * kp_30 + KP_30MIN_BLEND[1] * cur_kp

    # ---- 1-hour prediction ----
    # Average dominates; trend adds directional signal only
    bz_60   = _clamp((avg_bz    + bz_trend    * HORIZON_1HR) if avg_bz    is not None else None, BZ_MIN, BZ_MAX)
    spd_60  = _clamp((avg_speed + speed_trend * HORIZON_1HR) if avg_speed is not None else None, SPEED_MIN, SPEED_MAX)
    nhpi_60 = _clamp((avg_nhpi  + nhpi_trend  * HORIZON_1HR) if avg_nhpi  is not None else None, NHPI_MIN, NHPI_MAX)
    kp_60_trend = estimate_kp_from_conditions(bz_60, spd_60, nhpi_60)
    kp_60_avg   = estimate_kp_from_conditions(avg_bz, avg_speed, avg_nhpi)
    kp_60 = 0.3 * kp_60_trend + 0.7 * kp_60_avg
    if cur_kp is not None:
        kp_60 = KP_1HR_BLEND[0] * kp_60 + KP_1HR_BLEND[1] * cur_kp

    # Convert estimated Kp to visibility probability
    prob_5  = _kp_to_aurora_probability(kp_5,  geomag_lat)
    prob_30 = _kp_to_aurora_probability(kp_30, geomag_lat)
    prob_60 = _kp_to_aurora_probability(kp_60, geomag_lat)

    return {
        "timestamp": datetime.now(pytz.utc).isoformat(),
        "location": {
            "geo_lat": geo_lat,
            "geo_lon": geo_lon,
            "geomag_lat": round(geomag_lat, 2),
        },
        "current_conditions": {
            "bz": cur_bz,
            "bulk_speed": cur_speed,
            "north_hemi_power_index": cur_nhpi,
            "south_hemi_power_index": cur_shpi,
            "realtime_kp": cur_kp,
        },
        "trends_per_minute": {
            "bz_trend": round(bz_trend, 4),
            "speed_trend": round(speed_trend, 4),
            "nhpi_trend": round(nhpi_trend, 4),
            "shpi_trend": round(shpi_trend, 4),
        },
        "predictions": {
            "5min": {
                "estimated_kp": round(kp_5, 2),
                "aurora_probability": prob_5,
                "description": _probability_label(prob_5),
            },
            "30min": {
                "estimated_kp": round(kp_30, 2),
                "aurora_probability": prob_30,
                "description": _probability_label(prob_30),
            },
            "1hr": {
                "estimated_kp": round(kp_60, 2),
                "aurora_probability": prob_60,
                "description": _probability_label(prob_60),
            },
        },
    }


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

def _print_result(result):
    """Pretty-print a prediction result dict to stdout."""
    if "error" in result:
        print(f"  Error: {result['error']}")
        return

    loc    = result["location"]
    cond   = result["current_conditions"]
    trends = result["trends_per_minute"]
    preds  = result["predictions"]

    print(
        f"  Location : {loc['geo_lat']}°N, {loc['geo_lon']}°E  "
        f"(Geomagnetic Lat: {loc['geomag_lat']}°)"
    )
    print("  Current Conditions:")
    print(
        f"    Bz: {cond['bz']} nT | Speed: {cond['bulk_speed']} km/s | "
        f"NHPI: {cond['north_hemi_power_index']} GW | Kp: {cond['realtime_kp']}"
    )
    print("  Trends (per minute):")
    print(
        f"    Bz: {trends['bz_trend']:+.3f} nT/min | "
        f"Speed: {trends['speed_trend']:+.3f} km/s/min | "
        f"NHPI: {trends['nhpi_trend']:+.3f} GW/min"
    )
    print("  Aurora Visibility Predictions:")
    for horizon, pred in preds.items():
        print(
            f"    In {horizon:>4s}: Kp~{pred['estimated_kp']:.2f} | "
            f"Probability: {pred['aurora_probability']*100:.1f}% ({pred['description']})"
        )


# ---------------------------------------------------------------------------
# Sentinel runner
# ---------------------------------------------------------------------------

def run_sentinel(geo_lat, geo_lon, trigger_count=1, trigger_interval=60.0, db_path="aurora_data.db"):
    """
    Run the aurora sentinel service.

    The service queries the real-time data collector database and prints aurora
    visibility predictions for the given GPS location.  It repeats *trigger_count*
    times with *trigger_interval* seconds between each run.

    Args:
        geo_lat (float): Observer geographic latitude in degrees.
        geo_lon (float): Observer geographic longitude in degrees.
        trigger_count (int): Number of consecutive prediction runs.
        trigger_interval (float): Seconds to wait between consecutive runs.
        db_path (str): SQLite database file path.
    """
    for i in range(trigger_count):
        print(
            f"\n[Sentinel] Run {i + 1}/{trigger_count}  —  "
            f"{datetime.now(pytz.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC"
        )
        result = predict_aurora(geo_lat, geo_lon, db_path)
        _print_result(result)
        if i < trigger_count - 1:
            print(f"[Sentinel] Waiting {trigger_interval:.0f}s before next run…")
            time.sleep(trigger_interval)

    print("\n[Sentinel] Service completed.")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description=(
            "Aurora Sentinel Service — predicts aurora visibility probability "
            "at a GPS location based on real-time space-weather data."
        )
    )
    parser.add_argument(
        "--lat", type=float, required=True,
        help="Observer geographic latitude in degrees (e.g. 65.0).",
    )
    parser.add_argument(
        "--lon", type=float, required=True,
        help="Observer geographic longitude in degrees (e.g. 25.0).",
    )
    parser.add_argument(
        "--count", type=int, default=1,
        help="Number of consecutive trigger runs (default: 1).",
    )
    parser.add_argument(
        "--interval", type=float, default=60.0,
        help="Interval between trigger runs in seconds (default: 60).",
    )
    parser.add_argument(
        "--db", default="aurora_data.db",
        help="Path to the SQLite database populated by realtime_data_collector (default: aurora_data.db).",
    )

    args = parser.parse_args()
    run_sentinel(args.lat, args.lon, args.count, args.interval, args.db)


if __name__ == "__main__":
    main()

