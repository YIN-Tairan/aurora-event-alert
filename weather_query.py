import json
import time
import requests
from datetime import datetime, timedelta
from weatherCode import weatherCode, weatherCodeFullDay, weatherCodeDay, weatherCodeNight


# 读取 JSON 文件中的位置数据
def load_locations(file_path):
    with open(file_path, "r") as file:
        data = json.load(file)
    return data["locations"]

def get_weather_description(code, code_type="day"):
    # print("weather code is ", code, " str(code) is ", str(code))
    if code_type == "day":
        return weatherCode.get(str(code), "Unknown Weather")
    elif code_type == "full_day":
        return weatherCodeFullDay.get(str(code), "Unknown Weather")
    elif code_type == "night":
        return weatherCodeNight.get(str(code), "Unknown Weather")
    else:
        return "Invalid Code Type"

# 查询天气预报的函数
def get_weather_forecast(api_key, latitude, longitude, start_date, days):
    url = "https://api.tomorrow.io/v4/timelines"
    start_time = (datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days=1)).isoformat() + "Z"
    end_time = (datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days=days)).isoformat() + "Z"
    #print("start_time: ", start_time, "end_time: ", end_time)

    querystring = {
        "location": f"{latitude},{longitude}",
        "fields": ["temperatureMax", "temperatureMin", "humidityAvg", "weatherCode", "weatherCodeNight"],
        "timesteps": "1d",
        "startTime": start_time,
        "endTime": end_time,
        "units": "metric",
        "apikey": api_key
    }

    response = requests.get(url, params=querystring)
    if response.status_code == 200:
        data = response.json()
        return data['data']['timelines'][0]['intervals']
    else:
        return f"Error: {response.status_code}, {response.json()}"
    

def get_weather_forecast2(api_key, latitude, longitude, start_date, days):
    url = "https://api.tomorrow.io/v4/timelines"
    url = f"https://api.tomorrow.io/v4/weather/forecast?location={latitude}%2C%20{longitude}&timesteps=1d&apikey={api_key}"
    start_time = datetime.strptime(start_date, "%Y-%m-%d").isoformat() + "Z"
    end_time = (datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days=days)).isoformat() + "Z"
    #print("start_time: ", start_time, "end_time: ", end_time)

    headers = {"accept": "application/json"}

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        with open("weather.txt", "w") as file:
            file.write(str(data))
        return data['data']['timelines'][0]['intervals']
    else:
        return f"Error: {response.status_code}, {response.json()}"

def load_api_key(file_path):
    with open(file_path, "r") as file:
        data = json.load(file)
    return data["apikey"]

# 示例 API Key
#api_key = ""
api_key = load_api_key("keypwd.json")

# 打印天气信息
def print_weather_info(location, forecast):
    print(f"Weather forecast for {location['name']}:")
    for day in forecast:
        date = day['startTime'].split("T")[0]
        values = day['values']
        print(f"  Date: {date}")
        print(f"  Max Temperature: {values.get('temperatureMax')} °C")
        print(f"  Min Temperature: {values.get('temperatureMin')} °C")
        print(f"  Average Humidity: {values.get('humidityAvg')} %")
        print(f"  Day Weather: {get_weather_description(values.get('weatherCode', 0))}")
        print(f"  Night Weather: {get_weather_description(values.get('weatherCodeNight', 0), 'night')}")
        print("-" * 40)

good_visibility = {
  "10001": "Clear",
  "11001": "Mostly Clear",
  "11011": "Partly Cloudy",
  "11031": "Partly Cloudy and Mostly Clear",
  "21011": "Mostly Clear and Light Fog",
  "21021": "Partly Cloudy and Light Fog",
  "21061": "Mostly Clear and Fog",
  "42031": "Mostly Clear and Drizzle",
  "42131": "Mostly Clear and Light Rain",
  "51021": "Mostly Clear and Light Snow",
  "60031": "Mostly Clear and Freezing drizzle",
  "62131": "Mostly Clear and Freezing Rain",
  "71081": "Mostly Clear and Ice Pellets",
  "71101": "Mostly Clear and Light Ice Pellets"
}

def process_weather_info(location, forecast):
    report_txt = f"Weather forecast for {location['name']}:\n"
    good_condition = []
    condition_lvl = 0;
    for day in forecast:
        date = day['startTime'].split("T")[0]
        values = day['values']
        report_txt += f"  Date: {date}\n"
        report_txt += f"  Max Temperature: {values.get('temperatureMax')} °C\n"
        report_txt += f"  Min Temperature: {values.get('temperatureMin')} °C\n"
        report_txt += f"  Average Humidity: {values.get('humidityAvg')} %\n"
        report_txt += f"  Day Weather: {get_weather_description(values.get('weatherCode', 0))}\n"
        report_txt += f"  Night Weather: {get_weather_description(values.get('weatherCodeNight', 0), 'night')}\n"
        report_txt += "-" * 40 + "\n"
        night_visibility = values.get('weatherCodeNight', '0')
        night_good_visibility = good_visibility.get(str(night_visibility))
        if night_good_visibility:
            good_condition.append(True)
        else:
            good_condition.append(False)
    gc = good_condition
    if (len(gc)>=5 and gc[0] and gc[1] and gc[2] and gc[3] and gc[4]):
        condition_lvl = 4
    elif (len(gc)>=3 and gc[0] and gc[1] and gc[2]):
        condition_lvl = 3
    elif (len(gc)>=2 and gc[0] and gc[1]):
        condition_lvl = 2


    return condition_lvl, report_txt
# 主函数
def main():
    # api_key = "YOUR_API_KEY_HERE"
    #locations = load_locations("/home/ubuntu/projects/aurora/locations.json")
    locations = load_locations("./locations.json")
    start_date = datetime.now().strftime("%Y-%m-%d")  # 设定开始查询的日期
    
    report_output = ""
    for location in locations:
        forecast = get_weather_forecast(
            api_key,
            location["latitude"],
            location["longitude"],
            start_date,
            location["time_span"]
        )
        if isinstance(forecast, list):
            [c,t] = process_weather_info(location, forecast)
            report_output += f"Condition level {c} for {location['name']}\n"
            report_output += t
            report_output += "=" * 40 +"\n"
        else:
            report_output += f"Error fetching weather for {location['name']}: {forecast}\n"
            report_output += "=" * 40 + "\n"
        time.sleep(1)
        
    #print(report_output)
    return report_output

if __name__ == "__main__":
    main()
