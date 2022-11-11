import argparse
import os

default_file_path = "pii-anonymizer.json"


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config-file", help="config file to run the tool")
    args = parser.parse_args()

    if args.config_file:
        return args.config_file

    if os.path.exists(default_file_path):
        return default_file_path

    raise ValueError(
        f"Config file path or default file required. Please provide config file path or create {default_file_path}"
    )
