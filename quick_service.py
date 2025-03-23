import sqlite3
import argparse
from datetime import datetime, timedelta
import pytz

def arriving_solar_wind(db_path="aurora_data.db", timezone="UTC"):
    """
    提取 'forecast' 列中时间最接近当前时间的条目，并返回条目全文及字段意义。
    """
    try:
        # 当前时间
        utc_now = datetime.now(pytz.timezone('utc'))

        # 连接数据库
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        """cursor.execute("SELECT * FROM aurora_data")
        rows = cursor.fetchall()

        print("Printing the 5 first rows for debug")
        for row in rows[0:5]:
            print(row)
        print("Printing the 10 last rows for debug")
        for row in rows[-10:-1]:
            print(row)"""

        # 查询数据库中所有条目
        query = """
        SELECT * FROM aurora_data
        WHERE forecast IS NOT NULL
          AND NOT forecast = 'nan'
        ORDER BY ABS(strftime('%s', REPLACE(forecast, '_', ' ')) - strftime('%s', ?))
        LIMIT 1;
        """
        cursor.execute(query, (utc_now.strftime("%Y-%m-%d %H:%M:%S"),))
        result = cursor.fetchone()

        # 获取列名
        cursor.execute("PRAGMA table_info(aurora_data)")
        columns = [col[1] for col in cursor.fetchall()]


        conn.close()

        if not result:
            return "No data found in the database."

        # 将数据转换为字典
        entry = dict(zip(columns, result))

        # 解析时间并转换为指定时区
        tz = pytz.timezone(timezone)
        #forecast_time = datetime.strptime(entry["forecast"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=pytz.UTC)
        #entry["forecast"] = forecast_time.astimezone(tz).strftime("%Y-%m-%d %H:%M:%S %Z")
        forecast_time = datetime.strptime(entry["forecast"], "%Y-%m-%d_%H:%M").replace(tzinfo=pytz.UTC)
        entry["forecast"] = forecast_time.astimezone(tz).strftime("%Y-%m-%d %H:%M:%S %Z")

        # 字段意义
        field_meanings = {
            "id": "Primary key, unique identifier for the entry.",
            "datetime": "The datetime of the observation.",
            "modified_julian_day": "Modified Julian Day.",
            "seconds_of_day": "Seconds of the day.",
            "status": "Status code for the data quality.",
            "proton_density": "Proton density (p/cc).",
            "bulk_speed": "Bulk speed (km/s).",
            "ion_temperature": "Ion temperature (K).",
            "bx": "Magnetic field Bx component (nT).",
            "by": "Magnetic field By component (nT).",
            "bz": "Magnetic field Bz component (nT).",
            "bt": "Magnetic field total strength (nT).",
            "latitude": "Latitude of the measurement.",
            "longitude": "Longitude of the measurement.",
            "forecast": "Forecast time.",
            "north_hemi_power_index": "North Hemispheric Power Index (GigaWatts).",
            "south_hemi_power_index": "South Hemispheric Power Index (GigaWatts).",
            "realtime_kp": "Real-time Kp."
        }

        # 返回结果
        return {"entry": entry}

    except Exception as e:
        return f"Error: {e}"
    
def print_all_lines(db_path="aurora_data.db"):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM aurora_data")
    rows = cursor.fetchall()
    conn.close()
    for row in rows:
        print(row)
    

def main():
    parser = argparse.ArgumentParser(description="Quick Service CLI Tool")
    parser.add_argument("-fn", "--function", required=True, nargs='+', help="""Function name to execute. avalable function names: 
                                                                    1: asw for Arriving Solar Wind, 
                                                                    2: pal for Print All Lines """)
    parser.add_argument("-tz", "--timezone", default="UTC", help="Timezone for the output (default: UTC).")

    args = parser.parse_args()

    for fun in args.function:
        if fun == "asw":
            result = arriving_solar_wind(timezone=args.timezone)
            print("\nResult:")
            print(result)
        elif fun == "pal":
            print_all_lines()
        else:
            print(f"Function '{args.function}' not recognized.")

if __name__ == "__main__":
    main()