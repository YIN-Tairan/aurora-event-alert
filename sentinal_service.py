import sqlite3
import argparse
from datetime import datetime, timedelta
import pytz

TIME_ZONE = "UTC"

def latest_hour_data(db_path="aurora_data.db", timezone="UTC"):
    """
    查询数据库中表 aurora_data 最近一小时内的所有记录，并将 datetime 字段转换为指定时区的格式。
    
    假设数据库中 datetime 字段存储的格式为 pandas.to_datetime 默认的 ISO 格式，
    示例格式："YYYY-MM-DD HH:MM:SS" 或 "YYYY-MM-DD HH:MM:SS.ffffff"
    
    参数:
        db_path (str): SQLite 数据库文件路径，默认为 "aurora_data.db"。
        timezone (str): 目标时区，如 "UTC" 或 "Asia/Shanghai"，默认为 "UTC"。
    
    返回:
        List[dict]: 最近一小时数据，每个元素为包含所有字段的字典。
    """
    conn = None
    try:
        # 当前 UTC 时间及一小时前的时间
        utc_now = datetime.now(pytz.timezone('utc'))
        one_hour_ago = utc_now - timedelta(hours=1)
        
        # 使用 ISO 格式字符串（空格作为日期与时间分隔符），保持与 pandas.to_datetime 默认输出一致
        utc_now_str = utc_now.isoformat(' ')
        one_hour_ago_str = one_hour_ago.isoformat(' ')
        
        # 连接到数据库，并设置 row_factory 以返回字典类型数据
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # 以下为数据库中各字段的意义说明（仅用于文档参考）
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
        
        # 查询最近一小时内的数据，假定数据库 datetime 字段为字符串格式存储
        query = "SELECT * FROM aurora_data WHERE datetime BETWEEN ? AND ? ORDER BY datetime DESC"
        cursor.execute(query, (one_hour_ago_str, utc_now_str))
        rows = cursor.fetchall()
        
        # 将查询结果转换为列表字典格式
        data = [dict(row) for row in rows]
        
        # 如果目标时区不是 UTC，则对每条记录的 datetime 字段进行转换
        if timezone.upper() != "UTC":
            target_tz = pytz.timezone(timezone)
            for row in data:
                dt_str = row.get("datetime")
                if dt_str:
                    try:
                        # 使用 fromisoformat 解析 pandas 默认的 ISO 格式字符串
                        dt_obj = datetime.fromisoformat(dt_str)
                    except Exception:
                        # 备用方案：若解析失败，尝试用不含微秒的格式解析
                        dt_obj = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
                    # 如果解析出的 datetime 为天真（naive），假定其为 UTC
                    if dt_obj.tzinfo is None:
                        dt_obj = dt_obj.replace(tzinfo=pytz.utc)
                    # 转换到目标时区
                    dt_converted = dt_obj.astimezone(target_tz)
                    # 使用 ISO 格式输出（空格作为分隔符）
                    row["datetime"] = dt_converted.isoformat(' ')
        
        return data

    except Exception as e:
        print("Error:", e)
        return []
    finally:
        if conn:
            conn.close()

def realtime_kp_is_interesting(dataline):
    """
    判断实时 Kp 值是否有趣（大于等于 5）。
    
    参数:
        dataline (dict): 包含 "realtime_kp" 字段的字典。
    
    返回:
        bool: 实时 Kp 值是否有趣。
    """
    kp = dataline.get("realtime_kp")
    return kp is not None and kp >= 5

def predicted_nhpi_is_interesting(lines):
    """
    check if any line contains forecast arrival that is close to now and has interesting NHPI (>70)
    """
    now_time = datetime.now(tz=)
    for line in lines:
        forecast_time = datetime.strptime(line["forecast"], "%Y-%m-%d_%H:%M")
        if abs((forecast_time - now_time).total_seconds()) < 120 and line["north_hemi_power_index"] > 70:
            return [True, line]
    return [False, None]

def nowcasted_nhpi_is_interesting(dataline):
    """
    判断现在预测的北半球功率指数是否有趣（大于等于 100）。
    
    参数:
        dataline (dict): 包含 "north_hemi_power_index" 字段的字典。
    
    返回:
        bool: 现在预测的北半球功率指数是否有趣。
    """
    nhpi = dataline.get("north_hemi_power_index")
    return nhpi is not None and nhpi >= 10
    


if __name__ == "__main__":
    db_path = "aurora_data.db"
    timezone_input = input("请输入目标时区 (如 UTC 或 Asia/Shanghai，默认为 UTC): ").strip() or "UTC"
    results = latest_hour_data(db_path, timezone_input)
    print("查询结果：")
    for row in results:
        print(row)

