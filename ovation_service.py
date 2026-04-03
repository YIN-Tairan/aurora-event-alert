import json
import sqlite3
import numpy as np
import argparse
from datetime import datetime, timezone, timedelta

OVATION_DB_PATH = "ovation_aurora.db"


def bilinear_interpolation(lat, lon, coordinates_array):
    """
    Perform bilinear interpolation to estimate aurora intensity at (lat, lon).

    Parameters:
        lat (float): Latitude in degrees [-90, 90].
        lon (float): Longitude in degrees [0, 359].
        coordinates_array (np.ndarray): Array of shape (N, 3) with columns
            [longitude, latitude, aurora_intensity].

    Returns:
        float: Interpolated aurora intensity (probability 0-100).
    """
    longitudes = np.unique(coordinates_array[:, 0])
    latitudes = np.unique(coordinates_array[:, 1])

    # Build intensity grid shaped (n_lon, n_lat)
    intensity_grid = coordinates_array[:, 2].reshape(len(longitudes), len(latitudes))

    # Clamp to grid bounds to handle boundary coordinates gracefully
    lat = np.clip(lat, latitudes.min(), latitudes.max())
    lon = np.clip(lon, longitudes.min(), longitudes.max())

    # Find surrounding indices
    lon_idx = int(np.searchsorted(longitudes, lon, side="right")) - 1
    lat_idx = int(np.searchsorted(latitudes, lat, side="right")) - 1

    # Clamp indices so that idx+1 is always valid
    lon_idx = min(lon_idx, len(longitudes) - 2)
    lat_idx = min(lat_idx, len(latitudes) - 2)

    lon1, lon2 = longitudes[lon_idx], longitudes[lon_idx + 1]
    lat1, lat2 = latitudes[lat_idx], latitudes[lat_idx + 1]

    q11 = intensity_grid[lon_idx, lat_idx]
    q21 = intensity_grid[lon_idx + 1, lat_idx]
    q12 = intensity_grid[lon_idx, lat_idx + 1]
    q22 = intensity_grid[lon_idx + 1, lat_idx + 1]

    # Bilinear interpolation
    dlon = lon2 - lon1
    dlat = lat2 - lat1

    if dlon == 0 or dlat == 0:
        return float(q11)

    f_lat1 = ((lon2 - lon) / dlon) * q11 + ((lon - lon1) / dlon) * q21
    f_lat2 = ((lon2 - lon) / dlon) * q12 + ((lon - lon1) / dlon) * q22
    f = ((lat2 - lat) / dlat) * f_lat1 + ((lat - lat1) / dlat) * f_lat2
    return float(f)


def get_aurora_probability(lat, lon, db_path=OVATION_DB_PATH):
    """
    Query ovation_aurora.db and return predicted aurora probabilities at
    the specified GPS coordinates for now+5 min, now+30 min, and now+1 hour.

    The function:
      1. Loads all snapshots collected in the past hour.
      2. Filters those whose forecast_time falls within [now-15 min, now+1 hour].
      3. Bilinearly interpolates each snapshot's coordinate grid at (lat, lon).
      4. Interpolates across the time axis to predict probabilities at the
         three target forecast offsets.

    Parameters:
        lat (float): GPS latitude in degrees [-90, 90].
        lon (float): GPS longitude in degrees (any range; wrapped to [0, 359]).
        db_path (str): Path to the ovation_aurora SQLite database.

    Returns:
        dict with keys:
            "lat", "lon"                   – input coordinates
            "query_time_utc"               – UTC time of the query
            "prob_5min"                    – predicted probability at now+5 min
            "prob_30min"                   – predicted probability at now+30 min
            "prob_60min"                   – predicted probability at now+60 min
            "snapshots_used"               – number of snapshots used
        or None if no suitable data is found.
    """
    now = datetime.now(timezone.utc)
    t_window_start = now - timedelta(minutes=15)
    t_window_end = now + timedelta(hours=1)

    # Longitude: wrap to [0, 359]
    lon_grid = lon % 360

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Fetch snapshots collected in the past hour whose forecast_time is within
    # [now-15min, now+1hour].
    cursor.execute(
        """
        SELECT forecast_time, coordinates
        FROM ovation_aurora_snapshots
        WHERE observation_time >= strftime('%Y-%m-%dT%H:%M:%SZ', 'now', '-1 hour')
          AND forecast_time >= ?
          AND forecast_time <= ?
        ORDER BY forecast_time
        """,
        (
            t_window_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            t_window_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        ),
    )
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        return None

    # Build (offset_seconds, probability) pairs
    time_offsets = []
    probabilities = []

    for forecast_time_str, coordinates_json in rows:
        try:
            forecast_time = datetime.strptime(
                forecast_time_str, "%Y-%m-%dT%H:%M:%SZ"
            ).replace(tzinfo=timezone.utc)
        except ValueError:
            continue

        try:
            coordinates = json.loads(coordinates_json)
            coordinates_array = np.array(coordinates, dtype=float)
        except (json.JSONDecodeError, ValueError):
            continue

        try:
            prob = bilinear_interpolation(lat, lon_grid, coordinates_array)
        except Exception:
            continue

        offset_seconds = (forecast_time - now).total_seconds()
        time_offsets.append(offset_seconds)
        probabilities.append(prob)

    if len(time_offsets) < 2:
        return None

    time_offsets = np.array(time_offsets)
    probabilities = np.array(probabilities)

    # Sort by time offset
    sort_idx = np.argsort(time_offsets)
    time_offsets = time_offsets[sort_idx]
    probabilities = probabilities[sort_idx]

    # Target offsets: +5 min, +30 min, +60 min (in seconds from now)
    targets = {
        "prob_5min": 5 * 60,
        "prob_30min": 30 * 60,
        "prob_60min": 60 * 60,
    }

    predictions = {}
    for key, offset in targets.items():
        # np.interp clamps to boundary values outside the data range
        predictions[key] = float(np.interp(offset, time_offsets, probabilities))

    return {
        "lat": lat,
        "lon": lon,
        "query_time_utc": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "snapshots_used": len(time_offsets),
        **predictions,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Predict aurora probability at a GPS location using Ovation data."
    )
    parser.add_argument(
        "--lat",
        type=float,
        required=True,
        help="GPS latitude in degrees (e.g. 67.85572)",
    )
    parser.add_argument(
        "--lon",
        type=float,
        required=True,
        help="GPS longitude in degrees (e.g. 20.22513)",
    )
    parser.add_argument(
        "--db",
        type=str,
        default=OVATION_DB_PATH,
        help=f"Path to ovation_aurora SQLite database (default: {OVATION_DB_PATH})",
    )
    args = parser.parse_args()

    result = get_aurora_probability(args.lat, args.lon, db_path=args.db)

    if result is None:
        print(
            "No suitable Ovation snapshots found in the database for the requested "
            "time window. Make sure realtime_data_collector.py has been running."
        )
        return

    print(f"Query time (UTC)  : {result['query_time_utc']}")
    print(f"GPS coordinates   : lat={result['lat']}, lon={result['lon']}")
    print(f"Snapshots used    : {result['snapshots_used']}")
    print(f"Probability +5min : {result['prob_5min']:.2f}%")
    print(f"Probability +30min: {result['prob_30min']:.2f}%")
    print(f"Probability +60min: {result['prob_60min']:.2f}%")


if __name__ == "__main__":
    main()
