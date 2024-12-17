#!/home/ubuntu/venvs/aurora/bin/python3
import requests
import re
import csv
from datetime import datetime
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import weather_query
import json

class KPForecastError(Exception):
    """Custom exception for KP forecast errors"""
    pass

def load_email_pwd(file_path):
    with open(file_path, "r") as file:
        data = json.load(file)
    return data["smtppwd"]

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
        raise KPForCastError("无法找到KP值表格")

    kp_table_text = kp_values_match[0]

    # 解析KP值表格
    lines = kp_table_text.strip().split('\n')
    time_periods = []
    #kp_values = {date: [] for date in dates}
    # open a new file and write down the forecast data
    # 获取当前日期并格式化为字符串
    current_date = datetime.now().strftime('%Y-%m-%d')
    # 以当前日期为名创建csv文件
    path_prefix = "~/projects/aurora/data/"
    file_path = f"{path_prefix}{current_date}.csv"
    # 显式打开文件
    file_path = os.path.expanduser(file_path)
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
                        raise KPForecastError(f"Failed to open file: {e}")

                    if kp_value>=5:
                        kp5_detected = True
                    if kp_value>=7:
                        kp7_detected = True
            else:
                raise KPForecastError(f"Failed to exactly capture a 3-day value: {raw_parts}, filtered: {parts}")
                # throw error "the kp value detection failed to exactly capture a 3-day value, requiring further investigation. Detail: raw info:{raw_parts}, filtered info: {parts}"

    # 打印结果
    #for date in dates:
    #    print(f"日期：{date}")
    #    for i, kp_value in enumerate(kp_values[date]):
    #        print(f"  时段 {time_periods[i]}: Kp = {kp_value}")
    #    print()
    file.close()
    return [kp5_detected, kp7_detected, response.text]

if __name__ == "__main__":
    try:
        [kp5_bool, kp7_bool, text] = get_kp_forecast()
        print("KP 5 detected:", kp5_bool)
        print("KP 7 detected:", kp7_bool)
    except KPForecastError as e:
        error_message = str(e)
        print(f"Error occurred: {error_message}")
        send_error_email(error_message, MailInfo.default_receiver_email)

    weather_report_txt = weather_query.main()
    text = text + "\n"*5 + weather_report_txt

    # for debug purpose
    #kp5_bool = True
    # end

    send_highlight_email(kp5_bool, kp7_bool, text, MailInfo.default_receiver_email)
    send_highlight_email(kp5_bool, kp7_bool, text, "yin.tairan@outlook.com")

    if not kp5_bool and not kp7_bool:
        today = datetime.now()
        if today.weekday() == 6:
            send_report_email()
