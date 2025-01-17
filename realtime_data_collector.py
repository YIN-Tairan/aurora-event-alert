import requests
import json
import pandas as pd

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
    
    data = pd.DataFrame([line.split() for line in txt], columns=columns)
    # remove the last line which is none none none none
    data = data.dropna(how="all")
    return data

def merge_txt_data(data1, data2, data3):
    data1["Datetime"] = pd.to_datetime(data1[["Year", "Month", "Day"]].astype(str).agg("-".join, axis=1) + " " + data1["Time"].astype(str).str.zfill(4), format="%Y-%m-%d %H%M")
    data2["Datetime"] = pd.to_datetime(data2[["Year", "Month", "Day"]].astype(str).agg("-".join, axis=1) + " " + data2["Time"].astype(str).str.zfill(4), format="%Y-%m-%d %H%M")
    data3["Observation"] = pd.to_datetime(data3["Observation"], format="%Y-%m-%d_%H:%M")

    data1 = data1.drop(columns=["Year", "Month", "Day", "Time"])
    data2 = data2.drop(columns=["Year", "Month", "Day", "Time", "Modified Julian Day"])

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

    # 填充缺失值（如极光数据每5分钟记录一次，可用插值法补全）
    #merged_data.interpolate(method="linear", inplace=True)
    merged_data.fillna(method="ffill", inplace=True)  # 前向填充
    return merged_data

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
    print(merged_data.head())

    merged_data.to_csv("merged_data.csv", index=False)

    print("数据已保存为 'merged_data.csv'")