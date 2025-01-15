import requests
from skyfield.api import Loader, EarthSatellite
from datetime import datetime, timezone
import math

def fetch_tle(catnr):
    """
    从Celestrak获取指定CATNR的TLE数据。
    
    参数:
        catnr (int): NORAD目录号
    
    返回:
        tuple: (卫星名称, TLE Line 1, TLE Line 2) 或 None
    """
    url = f'https://celestrak.org/satcat/tle.php?CATNR={catnr}'
    response = requests.get(url)
    
    if response.status_code != 200:
        print(f"无法获取TLE数据，HTTP状态码: {response.status_code}")
        return None
    
    tle_lines = response.text.strip().splitlines()
    
    if len(tle_lines) < 3:
        print("TLE数据格式错误或不完整。")
        return None
    
    name = tle_lines[0].strip()
    line1 = tle_lines[1].strip()
    line2 = tle_lines[2].strip()
    
    return name, line1, line2

def calculate_distance_and_arrival_time(tle, solar_wind_speed=500):
    """
    计算ACE到地球表面的距离并估算太阳风到达时间。
    
    参数:
        tle (tuple): (卫星名称, TLE Line 1, TLE Line 2)
        solar_wind_speed (float): 太阳风速度，单位 km/s
    
    返回:
        dict: 包含时间、距离和到达时间的信息
    """
    name, line1, line2 = tle
    
    # 初始化Skyfield
    load = Loader('./skyfield_data')
    ts = load.timescale()
    
    # 获取当前UTC时间
    now = datetime.now(timezone.utc)
    t = ts.from_datetime(now)
    
    # 创建卫星对象
    satellite = EarthSatellite(line1, line2, name, ts)
    
    # 计算卫星的位置
    satellite_position = satellite.at(t)
    
    # 获取地球的半径（单位：公里）
    earth_radius = 6371.0
    
    # 计算ACE到地心的距离
    ace_distance_earth_center = satellite_position.distance().km
    
    # 计算ACE到地表的最短距离
    ace_distance_to_surface = ace_distance_earth_center - earth_radius
    
    # 估算太阳风到达时间
    arrival_time_seconds = ace_distance_to_surface / solar_wind_speed
    arrival_time_minutes = arrival_time_seconds / 60
    
    return {
        "current_time_utc": now,
        "distance_to_earth_center_km": ace_distance_earth_center,
        "distance_to_surface_km": ace_distance_to_surface,
        "arrival_time_seconds": arrival_time_seconds,
        "arrival_time_minutes": arrival_time_minutes
    }

def main():
    # ACE的NORAD目录号
    ACE_CATNR = 49685
    
    # 获取TLE数据
    tle = fetch_tle(ACE_CATNR)
    
    if tle is None:
        print("无法获取ACE的TLE数据。")
        return
    
    # 计算距离和到达时间
    results = calculate_distance_and_arrival_time(tle, solar_wind_speed=500)  # 可根据实际情况调整速度
    
    # 输出结果
    print(f"当前时间（UTC）：{results['current_time_utc']}")
    print(f"ACE到地心的距离：{results['distance_to_earth_center_km']:.2f} km")
    print(f"ACE到地表的距离：{results['distance_to_surface_km']:.2f} km")
    print(f"估算太阳风到达地球表面的时间：{results['arrival_time_seconds']:.2f} 秒 ({results['arrival_time_minutes']:.2f} 分钟)")

if __name__ == "__main__":
    main()
