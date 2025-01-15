import requests
import json
import argparse

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart











if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Use -t for servation duration, -f for frequence (service interval by minutes), -si for silent mode, -st for strong alert mode")
    # 添加互斥组 (si 和 st 互斥)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-si", "--silent", action="store_true", help="Enable silent mode (only report necessary alerts)")
    group.add_argument("-st", "--strong", action="store_true", help="Enable strong alert mode (always send summary at -f specified frequency)")

    # 添加其他参数
    parser.add_argument("-t", "--time", type=int, required=False, help="Observation duration in minutes")
    parser.add_argument("-f", "--frequency", type=int, required=False, help="Service frequency in minutes")

    # alert triggering conditions
    parser.add_argument()

    # 添加-h帮助信息
    #parser.add_argument("-h", "--help", action="help", help="-t {val} will start the service and last {val} minutes, default is 60min\n -f {val} will run the nowcast trackter every {val} minutes, default value is 5min\n -si runs the code in silent mode, only send alert when necessary\n -st sends report at frequency specified by -f no matter the current aurora condition")
    args = parser.parse_args()