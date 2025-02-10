import requests
import pandas as pd
import json
from datetime import datetime, timedelta
import time
from dateutil.parser import parse
import pytz
import argparse

# Amadeus API 配置
TOKEN_URL = "https://test.api.amadeus.com/v1/security/oauth2/token"
API_URL = "https://test.api.amadeus.com/v1/shopping/flight-destinations"

def load_api_key(file_path):
    with open(file_path, "r") as file:
        data = json.load(file)
    return data["amadeus_key"], data["amadeus_secret"]

[CLIENT_ID, CLIENT_SECRET] = load_api_key("keypwd.json")

# Step 1: 获取 OAuth2 访问令牌
def get_access_token():
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    
    data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }
    response = requests.post(TOKEN_URL, headers=headers, data=data)
    
    if response.status_code == 200:
        print("Token is: ", response.json()["access_token"])
        return response.json()["access_token"]
    else:
        raise Exception(f"获取令牌失败: {response.status_code} - {response.text}")

# Step 2: 将城市名称转换为机场代码（例如 "London" -> "LHR"）
def get_airport_code(city_name, token):
    url = "https://test.api.amadeus.com/v1/reference-data/locations"
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "subType": "CITY,AIRPORT",
        "keyword": city_name,
        "page[limit]": 1
    }
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        data = response.json()
        print(data)
        if data["data"]:
            return data["data"][0]["iataCode"]  # 返回第一个匹配的机场代码
        else:
            raise Exception(f"未找到城市 '{city_name}' 的机场代码")
    else:
        raise Exception(f"机场代码查询失败: {response.status_code} - {response.text}")
    
# Step 3: 解析航班详情
def parse_flight_details(flight):
    itineraries = flight["itineraries"]
    segments = []
    
    for itinerary in itineraries:
        for segment in itinerary["segments"]:
            segment_info = {
                "出发机场": segment["departure"]["iataCode"],
                "到达机场": segment["arrival"]["iataCode"],
                "起飞时间": segment["departure"]["at"],
                "降落时间": segment["arrival"]["at"],
                "航空公司": segment["carrierCode"],
                "航班号": segment["number"],
                "航段时长": segment["duration"]
            }
            segments.append(segment_info)
    
    return segments

def text_summary(result):
    txt = "="*20 +"  Flight information  "+"="*20+ "\n"
    txt += f"出发日期: {result['出发日期']}, "
    txt += f"返程日期: {result['返程日期']}, "
    txt += f"价格 (EUR): {result['价格 (EUR)']}, "
    txt += "航班详情:\n"
    for segment in result["航班详情"]:
        txt += f"出发机场: {segment['出发机场']}, 到达机场: {segment['到达机场']}, "
        txt += f"起飞时间: {segment['起飞时间']}, 降落时间: {segment['降落时间']}, "
        txt += f"航空公司: {segment['航空公司']}, 航班号: {segment['航班号']}, "
        txt += f"航段时长: {segment['航段时长']}"
        #txt += "-" * 40 + "\n"
        txt+="\n"
    return txt


# Step 4: 搜索航班并找到最低价选项
def search_cheapest_flight(token,origin, destination, departure_date, days=3):
    FLIGHT_SEARCH_URL = "https://test.api.amadeus.com/v2/shopping/flight-offers"
    # 计算返程日期范围
    departure_date_obj = parse(departure_date)
    return_date_start = departure_date_obj + timedelta(days=days)
    #return_date_end = departure_date_obj + timedelta(days=max_days)
    
    # 调用航班搜索 API
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "originLocationCode": origin,
        "destinationLocationCode": destination,
        "departureDate": departure_date,
        "returnDate": f"{return_date_start.strftime('%Y-%m-%d')}",#--{return_date_end.strftime('%Y-%m-%d')}",
        "adults": 1,
        "max": 50  # 最多返回 50 个结果
    }
    
    response = requests.get(FLIGHT_SEARCH_URL, headers=headers, params=params)
    
    if response.status_code == 200:
        data = response.json()
        #print(data)
        if not data.get("data"):
            raise Exception("未找到符合条件的航班")
        
        for offer in data["data"]:
            price = float(offer["price"]["total"])
            #if cheapest_flight is None or price < cheapest_flight["价格 (EUR)"]:
            if True:
                flight_info = { # cheapest_flight = ...
                    "出发日期": departure_date,
                    "返程日期": offer["itineraries"][1]["segments"][0]["departure"]["at"][:10],
                    "价格 (EUR)": price,
                    "航班详情": parse_flight_details(offer)
                }

                print(text_summary(flight_info))
        #return cheapest_flight
    else:
        raise Exception(f"航班搜索失败: {response.status_code} - {response.text}")


def process_segments_and_duration(itinerary):
    """
    计算单个行程的经停次数和总时长
    :param itinerary: 单个行程的JSON对象
    :return: (经停次数, 格式化后的总时长)
    """
    segments = itinerary['segments']
    num_segments = len(segments)
    
    if num_segments == 0:
        return num_segments, "N/A"
    
    duration_str = itinerary['duration']
    hours = 0
    minutes = 0
    if 'H' in duration_str:
        hours = float(duration_str.split('H')[0].split('T')[-1])
    if 'M' in duration_str:
        minutes = float(duration_str.split('M')[0].split('H')[-1])
    
    # 将分钟转换为小时
    total_hours = hours + (minutes / 60.0)
    return num_segments, total_hours
    
    
# service function
def search_flight(token,origin, destination, departure_date, days=3, max_price=450, max_layover=2, max_duration=15):
    FLIGHT_SEARCH_URL = "https://test.api.amadeus.com/v2/shopping/flight-offers"
    # 计算返程日期范围
    departure_date_obj = parse(departure_date)
    return_date_start = departure_date_obj + timedelta(days=days)
    #return_date_end = departure_date_obj + timedelta(days=max_days)

    # 调用航班搜索 API
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "originLocationCode": origin,
        "destinationLocationCode": destination,
        "departureDate": departure_date,
        "returnDate": f"{return_date_start.strftime('%Y-%m-%d')}",#--{return_date_end.strftime('%Y-%m-%d')}",
        "adults": 1,
        "max": 150  # 最多返回 个结果
    }
    
    response = requests.get(FLIGHT_SEARCH_URL, headers=headers, params=params)
    
    if response.status_code == 200:
        flight_offers = response.json()
        #print(data)
        if not flight_offers.get("data"):
            summary = ("未找到符合条件的航班")
            return summary, summary
        
        for offer in flight_offers["data"]:
            itinerary = offer['itineraries'][0]
            offer['segments_count'] = len(itinerary['segments'])  # 航段数
            offer['total_hours'] = process_segments_and_duration(itinerary)[1]  # 旅途时长
            offer['price_mark'] = float(offer['price']['total'])  # 总价格
            print(f"Debug: segments: {offer['segments_count']}, total hours: {offer['total_hours']}, total price: {offer['price_mark']}")
        
        # filter
        filtered_offers = list(filter(
            lambda x: (
                x['segments_count'] <= max_layover and  # 航段数不超过
                x['total_hours'] <= max_duration and    # 旅途时长不超过
                x['price_mark'] <= max_price             # 价格不超过
            ),
            flight_offers["data"]
        ))
        # 三阶排序
        sorted_offers = sorted(
            filtered_offers,
            key=lambda x: (x['segments_count'], x['total_hours'], x['price_mark'])
        )

        offer_count = 0
        short_summary = ""
        full_summary = ""
        for offer in sorted_offers:
            offer_count += 1
            
            flight_info = { # cheapest_flight = ...
                "出发日期": departure_date,
                "返程日期": offer["itineraries"][1]["segments"][0]["departure"]["at"][:10],
                "价格 (EUR)": offer["price"]["total"],
                "航班详情": parse_flight_details(offer)
            }

            flight_summary = text_summary(flight_info)

            if offer_count <= 10:
                short_summary += flight_summary
            full_summary += flight_summary
        return short_summary, full_summary

    else:
        summary = (f"航班搜索失败: {response.status_code} - {response.text}")
        
        return summary, summary
    
    

def flight_query(origin,dst,starting_dates, range_of_days, token=get_access_token(), timezone=pytz.timezone('utc')):
    # take as input the 3-letter code of departure and arrival airports, the range of days that you want to 
    # stay there, e.g. [3,4,5] means you are okay with staying either 3, 4 or 5 days at your destination
    # return a text summary of available flights: shortest duration, or cheapest price, or best compromise between the two factors, etc
    requests_number = len(starting_dates) * len(range_of_days)
    email_report=f"Flight offer between {origin} and {dst}: \n"+"Email version (for full report, check on the server)\n" + "="*30 +"\n"
    full_report=f"Flight offer between {origin} and {dst}: \n" + "Full report\n" + "="*30 + "\n"
    warn_txt = ""
    for date in starting_dates:
        if date == datetime.now(timezone).strftime("%Y-%m-%d"):
            warn_txt += f"Warning: You're trying to search a flight departing on the same day you are right now: {date}\n"
        #email_report += "*" * 30 + "\n" + " "*7 + f"Date: {date}\n" + "*"*30 + "\n"
        #full_report += "*" * 30 + "\n" + " "*7 + f"Date: {date}\n" + "*"*30 + "\n"
        email_report += warn_txt
        full_report += warn_txt
        for duration in range_of_days:
            email_report += "*" * 30 + "\n" + f"Departure: {date}, stay {duration} days\n" + "*"*30 + "\n"
            full_report += "*" * 30 + "\n" + f"Departure: {date}, stay {duration} days\n" + "*"*30 + "\n"
            [short_summary, full_summary] = search_flight(token, origin, dst, date, duration)
            # sleep 0.2 second to avoid frequency limits
            time.sleep(0.2)

            email_report += short_summary
            email_report += "#"*30 +"\n"

            full_report += full_summary
            full_report += "#"*30 +"\n"
    
    # TODO: add content here
    return email_report, full_report, requests_number



# 主程序
if __name__ == "__main__":
    #parser = argparse.ArgumentParser(description="")
    #parser.add_argument("-tk", "--token", required=False, default="" ,help="Provide existing token if you have one")

    #args = parser.parse_args()

    #try:
        # 输入参数
    origin = "CDG"  # 巴黎
    destination = "KEF"
    destination_city = "Reykjavik"#input("请输入目的地城市名称（例如 London）：")
    timezone = pytz.timezone('UTC')
    departure_date = (datetime.now(timezone)+timedelta(days=1)).strftime("%Y-%m-%d")#input("请输入出发日期（YYYY-MM-DD）：")
    
    
    # Step 2: 获取目的地机场代码
    #destination = get_airport_code(destination_city, token)
    #print(f"目的地机场代码: {destination}")
    # Step 3: 搜索最低价航班
    #[short_result, full_result] = search_flight(origin, destination, departure_date, token=token)
    [short_result, full_result,requests_count] = flight_query(origin, destination, [departure_date], [3])
    print(short_result)
    #import time
    #time.sleep(180)
    #print(f"total request count: {requests_count}")
    #print(full_result)

    
