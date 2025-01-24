import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime, timezone
import sqlite3
import smtplib
from email.mime.text import MIMEText
import json

# 配置信息
URL = "https://sidc.be/products/cactus/"  # 目标网页 URL
SMTP_SERVER = "smtp.gmail.com"          # 替换为你的 SMTP 服务器
SMTP_PORT = 587
EMAIL_USER = "tairan.yin.csdr@gmail.com"

RECIPIENT_EMAIL = "optimus.pascal.yin@gmail.com"     # 接收警报的邮箱

def load_email_pwd(file_path):
    with open(file_path, "r") as file:
        data = json.load(file)
    return data["smtppwd"]

EMAIL_PASSWORD = load_email_pwd("keypwd.json")

# 数据库初始化（记录已发送的 CME 事件）
conn = sqlite3.connect('cme_alerts.db')
cursor = conn.cursor()
cursor.execute('''
    CREATE TABLE IF NOT EXISTS sent_alerts (
        t0 TEXT PRIMARY KEY,
        event_date TEXT
    )
''')
conn.commit()

def fetch_html(url):
    """从 URL 获取网页内容"""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        # 显式设置编码（示例网页源码为 iso-8859-1）
        response.encoding = 'iso-8859-1'
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Error fetching URL: {e}")
        return None

def get_cme_alerts(html_content):
    """从网页内容提取 CME 事件"""
    soup = BeautifulSoup(html_content, 'html.parser')
    latest_header = soup.find('h2', {'name': 'Latest'})
    if not latest_header:
        return []
    
    alert_content = latest_header.find_next('pre')
    if not alert_content:
        return []
    
    data = alert_content.get_text()
    # 匹配所有 CME 事件的 t0 时间戳（示例格式：2025-01-21T09:24:07.532）
    pattern = re.compile(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3})')
    matches = pattern.findall(data)
    events = [{"t0": t0, "full_text": data} for t0 in matches]
    return events

def is_today_event(t0):
    """判断事件是否为当日发生（UTC 时间）"""
    try:
        event_date = datetime.fromisoformat(t0).date()
        current_date = datetime.now(timezone.utc).date()
        return event_date == current_date
    except ValueError:
        return False

def is_already_sent(t0):
    """检查是否已发送过该事件"""
    cursor.execute('SELECT t0 FROM sent_alerts WHERE t0 = ?', (t0,))
    return cursor.fetchone() is not None

def send_alert_email(event_count, event_text):
    """发送邮件警报"""
    subject = f"Halo CME Alert: New Event Detected ({event_count} Today)"
    body = f"""
    A new Halo CME has been detected today.\n
    Event details:\n
    {event_text}\n
    ---\n
    This is the {event_count}th CME today.
    """
    
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = EMAIL_USER
    msg['To'] = RECIPIENT_EMAIL
    
    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_USER, EMAIL_PASSWORD)
            server.sendmail(EMAIL_USER, RECIPIENT_EMAIL, msg.as_string())
        print("Alert email sent successfully.")
    except Exception as e:
        print(f"Failed to send email: {e}")

def main():
    # 1. 获取网页内容
    html_content = fetch_html(URL)
    if not html_content:
        return
    
    # 2. 提取并过滤当日未发送的事件
    events = get_cme_alerts(html_content)
    today_events = []
    
    for event in events:
        t0 = event["t0"]
        if is_today_event(t0) and not is_already_sent(t0):
            today_events.append(event)
            cursor.execute('INSERT INTO sent_alerts (t0, event_date) VALUES (?, ?)',
                         (t0, datetime.now(timezone.utc).isoformat()))
            conn.commit()
        # debug:
        #print(f"time: {t0}, text: {event["full_text"]}")
    
    # 3. 发送邮件（每个新事件单独通知）
    if today_events:
        for idx, event in enumerate(today_events, 1):
            send_alert_email(idx, event["full_text"])

    # debug test:
    #send_alert_email(1,events[0]["full_text"])

if __name__ == "__main__":
    
    main()
    conn.close()