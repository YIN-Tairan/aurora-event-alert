import requests
import json
import argparse

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


def report(args):
    """
    Prints all parsed arguments in a formatted manner.
    :param args: Parsed arguments from argparse.
    """
    print("\nParsed Arguments:")
    print("="*20)
    print(f"Observation Duration (-t): {args.time} minutes")
    print(f"Service Frequency (-f): {args.frequency} minutes")

    if args.silent:
        print("Mode (-si): Silent Mode Enabled")
    elif args.strong:
        print("Mode (-st): Strong Alert Mode Enabled")

    print(f"KP Threshold (-kp): {args.KP}")
    print(f"Solar Wind Speed Threshold (-v): {args.spped} km/s")
    print(f"North Hemispheric Power Index (-nhpi): {args.HP_north} Gigawatts")
    print(f"South Hemispheric Power Index (-shpi): {args.HP_south} Gigawatts")
    print(f"Proton Density Threshold (-pd): {args.proton_density} p/cc")

    if args.config_file:
        print(f"Config File (-config): {args.config_file}")
    else:
        print("Config File (-config): Not Specified")

    if args.addition_sent_to:
        print(f"Additional Emails (-add-email): {', '.join(args.addition_sent_to)}")
    else:
        print("Additional Emails (-add-email): Not Specified")

    print("="*20)


def load_config_file(config_file):
    """
    Load configuration from a JSON file and return it as a dictionary.
    :param config_file: Path to the configuration file.
    :return: Dictionary with configuration parameters.
    """
    try:
        with open(config_file, 'r') as file:
            config = json.load(file)
        return config
    except FileNotFoundError:
        print(f"Error: Config file '{config_file}' not found.")
        return {}
    except json.JSONDecodeError as e:
        print(f"Error: Failed to parse config file '{config_file}'. {e}")
        return {}

def update_args_with_config(args, config):
    """
    Update argparse arguments with values from the config file.
    :param args: Parsed arguments from argparse.
    :param config: Dictionary with configuration parameters.
    """
    print("Warning: a config file has been used and parameters will be override by the config file ")
    for key, value in config.items():
        if hasattr(args, key) and value!=getattr(args, key):
            print(f"Parameter override: {key}: {value} replaced origin value {getattr(args, key)}")
            setattr(args, key, value)







if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Use -t for servation duration, -f for frequence (service interval by minutes), -si for silent mode, -st for strong alert mode")
    # 添加互斥组 (si 和 st 互斥)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-si", "--silent", action="store_true", help="Enable silent mode (only report necessary alerts)")
    group.add_argument("-st", "--strong", action="store_true", help="Enable strong alert mode (always send summary at -f specified frequency)")
    group.add_argument("-db", "--debug", action="store_true", help="This will activate the debug mode and script behaviors will differ.")

    # 添加其他参数
    parser.add_argument("-t", "--time", type=int, required=False, default=60, help="Observation duration in minutes, default value 60min")
    parser.add_argument("-f", "--frequency", type=int, required=False, default=5, help="Service frequency in minutes, default value 5min")

    # alert triggering conditions
    parser.add_argument("-kp", "--KP", type=int, required=False, default=5, help="KP value of interest, int, default value is 5")
    parser.add_argument("-v", "--spped", type=float, required=False, default=500, help="Speed threshold for solar wind, default value is 500")
    parser.add_argument("-nhpi", "--HP-north", type=int, required=False, default=50, help="North hemispheric power index threshold, default value is 50")
    parser.add_argument("-shpi", "--HP-south", type=int, required=False, default=50, help="South hemispheric power index threshold, default value is 50")
    parser.add_argument("-pd", "--proton-density", type=float, required=False, default=10.0, help="Proton density threshold value, default value is 10.0")

    # load configure
    parser.add_argument("-config", "--config-file", type=str, required=False, default="", help="If a config file path is specified, ONLY parameters in the config file will be used, parameters entered by command line will NOT be taken into charge!")
    parser.add_argument("-add-email", "--addition-sent-to", type=str, nargs='+', required=False, default="", help="With or without these entry, the report message will be sent to optimus.pascal.yin@gmail.com")

    args = parser.parse_args()
    # Load and apply config file if specified
    if args.config_file:
        config = load_config_file(args.config_file)
        update_args_with_config(args, config)
    report(args)