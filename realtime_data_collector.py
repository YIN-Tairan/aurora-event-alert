import requests
import json
import pandas as pd
import sqlite3

# paths to download
p_intensity = "https://services.swpc.noaa.gov/json/ovation_aurora_latest.json" # used for ovation_vis
p_1min_kp_boulder = "https://services.swpc.noaa.gov/json/boulder_k_index_1m.json" # Boulder is a city in Colorado...
p_1min_kp_planetary = "https://services.swpc.noaa.gov/json/planetary_k_index_1m.json" # real-time kp value
p_nowcast = "https://services.swpc.noaa.gov/text/aurora-nowcast-hemi-power.txt"
p_swepam = "https://services.swpc.noaa.gov/text/ace-swepam.txt"
p_mag = "https://services.swpc.noaa.gov/text/ace-magnetometer.txt"

# constant
COLUMNS_SWEPAM = [
    "Year", "Month", "Day", "Time", "Modified Julian Day", "Seconds of Day",
    "Status", "Proton Density", "Bulk Speed", "Ion Temperature"
    ]

COLUMNS_NOWCAST = [
    "Observation", "Forecast", 
    "North-Hemispheric-Power-Index (GigaWatts)", 
    "South-Hemispheric-Power-Index (GigaWatts)"
]

COLUMNS_MAG = [
    "Year", "Month", "Day", "Time", "Modified Julian Day", "Seconds of Day",
    "Status", "Bx", "By", "Bz", "Bt", "Latitude", "Longitude"
]


def load_text_file(url):
    response = requests.get(url)
    response.raise_for_status()  # 检查请求是否成功

    data = response.text
    return data

def load_json_file(url):
    response = requests.get(url)
    response.raise_for_status()  # 检查请求是否成功
    data = json.loads(response.text)
    return data

def crop_txt_header(txt):
    # 提取非注释行（不以#开头）
    data_lines = txt.split("\n")
    data_lines = [line for line in data_lines if not (line.startswith(":") or line.startswith("#"))]

    # 去掉可能的空行并从第一行包含数据的地方开始
    #data_lines = [line.strip() for line in data_lines if line.strip()]
    return data_lines

def extract_txt_table(txt, columns):
    
    #data = pd.DataFrame([line.split() for line in txt], columns=columns)
    valid_data = [
        line.split() for line in txt if len(line.split()) == len(columns)
    ]
    data = pd.DataFrame(valid_data, columns=columns)
    # remove the last line which is none none none none
    data = data.dropna(how="all")
    return data

def merge_txt_data(data1, data2, data3):
    data1["Datetime"] = pd.to_datetime(data1[["Year", "Month", "Day"]].astype(str).agg("-".join, axis=1) + " " + data1["Time"].astype(str).str.zfill(4), format="%Y-%m-%d %H%M")
    data2["Datetime"] = pd.to_datetime(data2[["Year", "Month", "Day"]].astype(str).agg("-".join, axis=1) + " " + data2["Time"].astype(str).str.zfill(4), format="%Y-%m-%d %H%M")
    data3["Observation"] = pd.to_datetime(data3["Observation"], format="%Y-%m-%d_%H:%M")

    data1 = data1.drop(columns=["Year", "Month", "Day", "Time"])
    data2 = data2.drop(columns=["Year", "Month", "Day", "Time", "Modified Julian Day", "Seconds of Day"])

    # 生成完整的时间序列
    start_time = data1["Datetime"].min()
    end_time = data1["Datetime"].max()
    time_index = pd.date_range(start=start_time, end=end_time, freq="1min")  # 每分钟数据

    # 创建一个基准 DataFrame
    base_df = pd.DataFrame({"Datetime": time_index})

    # 按时间戳合并数据
    merged_data = base_df.merge(data1, on="Datetime", how="left") \
                         .merge(data2, on="Datetime", how="left") \
                         .merge(data3, left_on="Datetime", right_on="Observation", how="left")
    
    merged_data= merged_data.drop(columns=["Observation"])

    # 填充缺失值（如极光数据每5分钟记录一次，可用插值法补全）
    #merged_data.interpolate(method="linear", inplace=True)
    #merged_data.fillna(method="ffill", inplace=True)  # 前向填充
    merged_data.ffill(inplace=True)
    return merged_data

def insert_data_ignore(data, db_path="aurora_data.db"):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    query = """
    INSERT OR IGNORE INTO aurora_data (
        datetime, modified_julian_day, seconds_of_day, status_x,
        proton_density, bulk_speed, ion_temperature, status_y, bx, by, bz, bt,
        latitude, longitude, forecast, north_hemi_power_index, south_hemi_power_index
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    # 将 DataFrame 转换为元组列表
    values = [
        (
            row["datetime"], row["modified_julian_day"], row["seconds_of_day"], row["Status_x"],
            row["proton_density"], row["bulk_speed"], row["ion_temperature"], row["Status_y"], row["bx"], row["by"],
            row["bz"], row["bt"], row["latitude"], row["longitude"], row["forecast"],
            row["north_hemi_power_index"], row["south_hemi_power_index"]
        )
        for _, row in data.iterrows()
    ]
    cursor.executemany(query, values)
    conn.commit()
    conn.close()

if __name__ == "__main__":
    data_nowcast = load_text_file(p_nowcast)
    data_nowcast_lines = crop_txt_header(data_nowcast)
    data3 = extract_txt_table(data_nowcast_lines, COLUMNS_NOWCAST)

    data_swepam = load_text_file(p_swepam)
    data_swepam_lines = crop_txt_header(data_swepam)
    data1 = extract_txt_table(data_swepam_lines, COLUMNS_SWEPAM)

    data_mag = load_text_file(p_mag)
    data_mag_lines = crop_txt_header(data_mag)
    data2 = extract_txt_table(data_mag_lines, COLUMNS_MAG)
    #for line in txt:
    #    print(line)
    merged_data = merge_txt_data(data1,data2,data3)

    #print(merged_data.head())

    merged_data.to_csv("merged_data.csv", index=False)
    print("数据已保存为 'merged_data.csv'")


    merged_data.rename(columns={
        "Datetime": "datetime",
        "Modified Julian Day": "modified_julian_day",
        "Seconds of Day": "seconds_of_day",
        "Proton Density": "proton_density",
        "Bulk Speed": "bulk_speed",
        "Ion Temperature": "ion_temperature",
        "Bx": "bx",
        "By": "by",
        "Bz": "bz",
        "Bt": "bt",
        "Latitude": "latitude",
        "Longitude": "longitude",
        "Forecast": "forecast",
        "North-Hemispheric-Power-Index (GigaWatts)": "north_hemi_power_index",
        "South-Hemispheric-Power-Index (GigaWatts)": "south_hemi_power_index"
    }, inplace=True)



    conn = sqlite3.connect("aurora_data.db")

    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS aurora_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        datetime TEXT NOT NULL UNIQUE,
        modified_julian_day INTEGER,
        seconds_of_day INTEGER,
        status_x INTEGER,
        proton_density REAL,
        bulk_speed REAL,
        ion_temperature REAL,
        status_y,
        bx REAL,
        by REAL,
        bz REAL,
        bt REAL,
        latitude REAL,
        longitude REAL,
        forecast TEXT,
        north_hemi_power_index REAL,
        south_hemi_power_index REAL
    )
    """)
    # debug
    cursor.execute("SELECT COUNT(*) FROM aurora_data")
    row_count = cursor.fetchone()[0]
    # 打印结果
    print(f"数据库表中共有 {row_count} 条记录。")

    #merged_data.to_sql("aurora_data", conn, if_exists="append", index=False, method=)
    merged_data['datetime'] = merged_data['datetime'].astype(str)
    merged_data['forecast'] = merged_data['forecast'].astype(str)
    insert_data_ignore(merged_data)

    # 关闭连接
    #conn.close()

    print("数据已成功插入数据库")

    #debug
    cursor.execute("SELECT COUNT(*) FROM aurora_data")
    row_count = cursor.fetchone()[0]
    # 打印结果
    print(f"数据库表中共有 {row_count} 条记录。")


    