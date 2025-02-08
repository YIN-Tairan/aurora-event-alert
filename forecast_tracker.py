#!/home/ubuntu/venvs/aurora/bin/python3
import requests
import re
import csv
from datetime import datetime, timedelta, date
import pytz
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import weather_query
import json
import argparse
import numpy as np
import travel


class KPForecastError(Exception):
    """Custom exception for KP forecast errors"""
    pass

def load_email_pwd(file_path):
    with open(file_path, "r") as file:
        data = json.load(file)
    return data["smtppwd"]

def save_string_to_txt(content, file_path):
    """
    将字符串保存为txt文件
    :param content: 要保存的字符串内容
    :param file_path: 文件保存路径（包括文件名，如"output.txt"）
    """
    try:
        with open(file_path, 'w', encoding='utf-8') as file:
            file.write(content)
        print(f"文件已成功保存至: {file_path}")
    except Exception as e:
        print(f"保存文件时出错: {e}")

class MailInfo:
    sender_email = "tairan.yin.csdr@gmail.com"
    default_receiver_email = "optimus.pascal.yin@gmail.com"
    # Gmail SMTP 服务器配置
    smtp_server = "smtp.gmail.com"
    smtp_port = 587
    smtp_username = sender_email
    #smtp_password = ""  # 使用 Gmail 应用密码
    smtp_password = load_email_pwd("keypwd.json")


def send_error_email(error_message, receiver_email):
    # 配置邮件发送
    sender_email = MailInfo.sender_email
    subject = "KP Forecast Error"
    # SMTP 服务器配置
    smtp_server = MailInfo.smtp_server
    smtp_port = MailInfo.smtp_port
    smtp_username = sender_email
    smtp_password = MailInfo.smtp_password  # 使用 Gmail 应用密码
    

    # 创建邮件
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = "Aurora forecaset script has encountered an error"

    # 邮件正文
    body = f"An error occurred during the KP forecast retrieval: {error_message}"
    msg.attach(MIMEText(body, 'plain'))

    # 发送邮件
    try:
        # 连接到 Gmail 的 SMTP 服务器
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()  # 启用 TLS 加密
        server.login(smtp_username, smtp_password)
        server.sendmail(sender_email, receiver_email, msg.as_string())
        print(f"Error email sent to {receiver_email}")
    except Exception as e:
        print(f"Failed to send error email: {e}")
    finally:
        server.quit()


def send_highlight_email(kp5_detected, kp7_detected, text, receiver_email):
    # 如果 kp5_detected 和 kp7_detected 都为 False，什么也不做
    if not kp5_detected and not kp7_detected:
        return
    # 配置邮件发送
    sender_email = MailInfo.sender_email
    subject = "KP Forecast Error"
    # SMTP 服务器配置
    smtp_server = MailInfo.smtp_server
    smtp_port = MailInfo.smtp_port
    smtp_username = sender_email
    smtp_password = MailInfo.smtp_password  # 使用 Gmail 应用密码

    # 根据 kp5_detected 和 kp7_detected 设置邮件主题
    if kp7_detected:
        subject = "[Urgent Aurora Report] A remarkable aurora might happen in the next 3 days"
    elif kp5_detected:
        subject = "[Aurora Report] A kp5+ forecast has been detected for the next days"

    # 创建邮件
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = subject

    # 邮件正文内容
    text = "for more information check: https://www.swpc.noaa.gov/communities/aurora-dashboard-experimental \n\n" + text
    msg.attach(MIMEText(text, 'plain'))

    # 发送邮件
    try:
        # 连接到 Gmail 的 SMTP 服务器
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()  # 启用 TLS 加密
        server.login(smtp_username, smtp_password)
        server.sendmail(sender_email, receiver_email, msg.as_string())
        print(f"Email sent with subject: {subject}")
    except Exception as e:
        print(f"Failed to send email: {e}")
    finally:
        server.quit()

def send_report_email():
    # 配置邮件发送
    sender_email = MailInfo.sender_email
    subject = "KP Forecast Error"
    receiver_email = MailInfo.default_receiver_email
    # SMTP 服务器配置
    smtp_server = MailInfo.smtp_server
    smtp_port = MailInfo.smtp_port
    smtp_username = sender_email
    smtp_password = MailInfo.smtp_password  # 使用 Gmail 应用密码

    subject = "[Script Report] Aurora monitoring script is running"
    body = "This is a weekly report to confirm that the aurora monitoring script is running correctly, but no significant kp5+ forecast has been detected in the past week."

    # 创建邮件
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    # 发送邮件
    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()  # 启用 TLS 加密
        server.login(smtp_username, smtp_password)
        server.sendmail(sender_email, receiver_email, msg.as_string())
        print(f"Weekly report email sent to {receiver_email}")
    except Exception as e:
        print(f"Failed to send weekly report email: {e}")
    finally:
        server.quit()

def parse_and_complete_date(month_day_str, current_date=None):
    """
    将 'Jan 25' 格式的日期补全年份，并转换为 'yyyy-mm-dd'。
    补全年份的逻辑：
      - 如果当前月份是 12 月且目标月份是 1 月，年份设为下一年。
      - 其他情况年份设为当前年。
    
    :param month_day_str: 输入的月日字符串，如 'Jan 25'
    :param current_date: 当前日期（默认为当前系统时间）
    :return: 格式化的完整日期字符串，如 '2025-01-25'
    """
    # 如果没有指定当前日期，则使用系统当前时间
    if current_date is None:
        current_date = datetime.now()
    
    # 解析输入的月日字符串
    try:
        parsed_date = datetime.strptime(f"{month_day_str} 2000", "%b %d %Y")
    except ValueError:
        raise ValueError("日期格式错误，应为 'MMM DD'（如 'Jan 25'）")
    
    target_month = parsed_date.month
    target_day = parsed_date.day
    current_year = current_date.year
    current_month = current_date.month
    
    # 判断是否需要将年份设为下一年
    if current_month == 12 and target_month == 1:
        year = current_year + 1
    else:
        year = current_year
    
    # 尝试构建日期，处理无效日期（如闰年2月29日）
    try:
        final_date = datetime(year, target_month, target_day)
    except ValueError as e:
        raise ValueError(f"无效日期: {year}-{target_month:02d}-{target_day:02d}") from e
    
    return final_date.strftime("%Y-%m-%d")


def get_kp_forecast():
    url = 'https://services.swpc.noaa.gov/text/3-day-forecast.txt'
    response = requests.get(url)
    response.raise_for_status()  # 检查请求是否成功

    data = response.text

    # 使用正则表达式提取KP指数部分
    kp_section_pattern = r'NOAA Kp index breakdown.*?B. NOAA Solar Radiation'
    kp_section = re.search(kp_section_pattern, data, re.DOTALL)
    if not kp_section:
        raise KPForecastError("无法找到KP指数预报部分")

    kp5_detected = False
    kp7_detected = False

    kp_section_text = kp_section.group()

    # 提取日期
    dates_pattern = r'NOAA Kp index breakdown (.*?)\n'
    dates_match = re.search(dates_pattern, kp_section_text)
    if not dates_match:
        raise KPForecastError("无法找到日期信息")

    #print("kp_section ",kp_section.group())
    #print("kp_section_text ", kp_section_text)
    #print(dates_match)
    #print("dates_group0 ", dates_match.group(0))
    #print("dates_group1 ", dates_match.group(1))
    #print("dates_group_all ", dates_match.group())

    # find the line with three dates
    first_date = dates_match.group(1).split('-')[0]
    three_dates_pattern = r'{} (.*?)\n'.format(re.escape(first_date))
    three_dates_match = re.search(three_dates_pattern, kp_section_text)
    three_dates_line = three_dates_match.group().strip()
    #print("match results:",three_dates_match.group())
    ##dates = three_dates_line.split()
    dates = re.split(r'\s{2,}', three_dates_line)
    ##print("dates: ", dates)

    # 提取KP值表格
    kp_values_pattern = r'(?:\d{2}-\d{2}UT.*?)\n\n'
    kp_values_match = re.findall(kp_values_pattern, kp_section_text, re.DOTALL)
    if not kp_values_match:
        raise KPForecastError("无法找到KP值表格")

    kp_table_text = kp_values_match[0]

    # 解析KP值表格
    lines = kp_table_text.strip().split('\n')
    time_periods = []
    #kp_values = {date: [] for date in dates}
    # open a new file and write down the forecast data
    # 获取当前日期并格式化为字符串
    current_date = datetime.now().strftime('%Y-%m-%d')
    # 以当前日期为名创建csv文件
    path_prefix = "./data/"
    file_path = f"{path_prefix}{current_date}.csv"
    # 显式打开文件
    file_path = os.path.expanduser(file_path)
    print(file_path)

    # create an array to store kp value
    kp_matrix = np.zeros([8,3])
    line_idx = 0

    # create a file to save the text kp forecast
    file = open(file_path, mode='w', newline='', encoding='utf-8')
    writer = csv.writer(file)
    writer.writerow(['date','D-Day','D+1','D+2'])
    for line in lines:
        if line.startswith('00-03UT') or line.startswith('03-06UT') or line.startswith('06-09UT') or line.startswith('09-12UT') or line.startswith('12-15UT') or line.startswith('15-18UT') or line.startswith('18-21UT') or line.startswith('21-00UT'):
            raw_parts = line.strip().split()
            parts = [part for part in raw_parts if re.match(r'^-?\d+(\.\d+)?$', part)]
            # print("Filtered KP parts:", parts)  # 打印过滤后的值
            if len(parts) == 3:
                writer.writerow([raw_parts[0], parts[0], parts[1], parts[2]])
                time_period = raw_parts[0] # time, e.g., '00-03UT'
                time_periods.append(time_period)
                for i, date in enumerate(dates):
                    #print("i ",i, " date", date, "part length", len(parts))
                    kp_string = parts[i]
                    #kp_values[date].append(kp_value)
                    try:
                        kp_value = float(kp_string)
                    except ValueError:
                        raise KPForecastError(f"Cannot convert the kp_value string into float, kp_value is {kp_value} but is supposed to be a string of float")
                    
                    #store the kp value in kp_matrix[line_idx,i]
                    kp_matrix[line_idx, i] = kp_value

                    # this part is removed because a newer detection logic has been used further below
                    #if kp_value>=5:
                    #    kp5_detected = True
                    #if kp_value>=7:
                    #    kp7_detected = True

                line_idx += 1
            else:
                raise KPForecastError(f"Failed to exactly capture a 3-day value: {raw_parts}, filtered: {parts}")
                # throw error "the kp value detection failed to exactly capture a 3-day value, requiring further investigation. Detail: raw info:{raw_parts}, filtered info: {parts}"

    file.close()

    # check the presence of kp5 and kp7m (7m: 7 minus, i.e., >=6.67)
    interesting_dates = []
    #debug print kp_matrix
    #print(kp_matrix)
    for i, date in enumerate(dates):
        #print(i,date)
        #print(kp_matrix[:,i])
        if (kp_matrix[:,i]>=5).any():
            kp5_detected = True
            interesting_dates.append(parse_and_complete_date(date))
        if (kp_matrix[:,i]>=6.67).any():
            kp7_detected = True
        
        
    return [kp5_detected, kp7_detected, response.text, interesting_dates]

def replace_date_with_annotation(original_str, target_date):
    """
    将目标日期替换为 "yyyy-mm-dd is DATE OF INTEREST"。
    示例：
       输入：原文字符串 = "事件发生在 2025-01-25", 目标日期 = "2025-01-25"
       输出：事件发生在 2025-01-25 is DATE OF INTEREST
    """
    # 直接替换目标日期字符串
    #print("origin txt:", original_str)
    #print("modified txt: ", original_str.replace(target_date, f"{target_date} is DATE OF INTEREST"))
    return original_str.replace(target_date, f"{target_date}(DATE OF KP) ")

def flight_query_js_process(js_dict):
    # this js_dict is a modified "locations.js" provided by weather_query
    good_condition_dst = []
    for location in js_dict:
        condition_code = location["forecast_condition_code"]
        if condition_code>0:
            good_condition_dst.append(location["airport_code"])
    return good_condition_dst

def previous_day(input_date_str):
    # 将输入的字符串转换为日期对象
    input_date = datetime.strptime(input_date_str, "%Y-%m-%d").date()
    previous_day = input_date - timedelta(days=1)
    
    # 获取当前日期
    tz = pytz.timezone('utc')
    current_date = datetime.now(tz).date()
    if previous_day < current_date:
        print(f"Warning: the date {previous_day} is discard being earlier than today")
        return None
    else:
        return previous_day.strftime("%Y-%m-%d")
    
def local_flight_info_exist():
    file_path = "flight_info.txt"
    
    # 检查文件是否存在
    if not os.path.exists(file_path):
        return False
    
    try:
        # 读取文件第一行
        with open(file_path, "r", encoding="utf-8") as f:
            first_line = f.readline().strip()
    except Exception as e:  # 处理权限错误、文件损坏等异常
        print(f"Error reading file: {e}")
        return False
    
    # 获取当前UTC日期字符串
    try:
        utc_date = datetime.now(pytz.utc).date().strftime("%Y-%m-%d")
    except Exception as e:
        print(f"Error getting UTC date: {e}")
        return False
    
    # 比较并返回结果
    return first_line == utc_date


def main(check_weather=True, send_email=True, flight_query=True, print_report=False):    
    
    text = ""
    try:
        [kp5_bool, kp7_bool, noaa_txt, interesting_dates] = get_kp_forecast()
        print("KP 5 detected:", kp5_bool)
        print("KP 7 detected:", kp7_bool)
        print("interesting dates are: ", interesting_dates)
    except KPForecastError as e:
        error_message = str(e)
        print(f"Error occurred: {error_message}")
        send_error_email(error_message, MailInfo.default_receiver_email)


    if check_weather:
        [weather_report_txt, locations_js] = weather_query.query_wether()
    else:
        weather_report_txt = ""
    for date in interesting_dates:
        weather_report_txt = replace_date_with_annotation(weather_report_txt,date)

    if flight_query and check_weather: # flight query requires the js_dict output provided by weather query
        if not local_flight_info_exist():
            short_summary = datetime.now(pytz.timezone('utc')).date().strftime("%Y-%m-%d") + "\n"
            long_summary = datetime.now(pytz.timezone('utc')).date().strftime("%Y-%m-%d") + "\n"
            origin = "CDG" # The default departure airport is CDG
            dst_airport_code = flight_query_js_process(locations_js)
            duration = [3,4,5]
            start_dates = []
            for date in interesting_dates:
                d_minus_1 = previous_day(date)
                if d_minus_1 is not None:
                    start_dates.append(d_minus_1)
                    print(f"Debug: flight query will take {d_minus_1} into the search")
            api_calls_count = 0
            for dst in dst_airport_code:
                # the flight query function takes a list of start time and a list of possible durations, but one destination at a time
                [short_report, long_report, call_count] = travel.flight_query(origin, dst, start_dates, duration)
                api_calls_count += call_count
                short_summary += short_report
                long_summary += long_report
            
            flight_query_txt = short_summary
            with open ("flight_info.txt", "w", encoding="utf-8") as f:
                f.write(long_summary)
            with open ("flight_info_email.txt", "w", encoding="utf-8") as fe:
                fe.write(short_summary)
            print(f"Total api calls during this query: {api_calls_count}")
        else:
            with open ("flight_info_email.txt", "r", encoding="utf-8") as fe:
                flight_query_txt = fe.read()
            print("Amadeus API is not used in the flight query. Using local flight offers report created earlier this day.")
    elif not check_weather:
        print("Warning: the flight query is ignored because the weather query is not activated")
        flight_query_txt = ""


        

                


       



    text = noaa_txt + "\n"*5 + weather_report_txt + "\n"*3 + flight_query_txt + "////End of Report\n////"

    if print_report:
        print(text)
    # for debug purpose
    #kp5_bool = True
    # end
    if send_email:
        send_highlight_email(kp5_bool, kp7_bool, text, MailInfo.default_receiver_email)
        send_highlight_email(kp5_bool, kp7_bool, text, "yin.tairan@outlook.com")
    # else do nothing

    # debug print to test the time function
    #today = datetime.now(tz=pytz.utc)
    #time_now = today.time()
    #print(time_now)
    if not kp5_bool and not kp7_bool:
        today = datetime.now(tz=pytz.utc)
        time_now = today.time()
        start_time = datetime.time(7, 0, 0)
        end_time = datetime.time(12, 0, 0)
        
        if today.weekday() == 6 and start_time <= time_now <= end_time:
            send_report_email()

def main_normal():
    main()

def main_debug(arg_list):
    if "noEmail" in arg_list:
        sendEmail = False
        print("Debug mode: not sending email.")
    else:
        sendEmail = True

    if "noWeather" in arg_list:
        check_weather = False
        print("Debug mode: not checking weather report.")
    else:
        check_weather = True

    if "noFlight" in arg_list:
        flight_query = False
    else:
        flight_query = True

    if "noReport" in arg_list:
        print("Debug mode: the txt report will be printed to terminal")
        print_report=False
    else:
        print_report=True

    if "failedCase" in arg_list:
        raise Exception("[Debug] This is an faked error message.")
    
    
    main(send_email=sendEmail, check_weather=check_weather, flight_query=flight_query,print_report=print_report)
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="示例：-n 或 -d [多个debug内容]")
    #group = parser.add_mutually_exclusive_group(required=True)
    group = parser.add_mutually_exclusive_group(required=False)

    group.add_argument(
        "-n", "--normal",
        action="store_true",
        help="normal 模式"
    )

    # nargs='+' 表示可以接收一个或多个参数，并作为list返回
    group.add_argument(
        "-d", "--debug",
        nargs='+',
        help="debug 模式，可以接收一个或多个调试参数"
    )

    

    args = parser.parse_args()

     # 如果没有输入任何参数，默认设置为 normal 模式
    if not any(vars(args).values()):
        args.normal = True

    if args.normal:
        main_normal()
    elif args.debug:
        main_debug(args.debug)  # 这是一个list

