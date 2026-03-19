import pandas as pd
import numpy as np
import logging
import sys
import requests
import json

logger = logging.getLogger(__name__)


"""
pipeline.py:
    main run
    logs
    ingesting API data for both
    write to parquet/csv/json
clean.py:
    data validation stuff
transform.py
    transformation
    summary
"""

def setup_logging():

    logger.setLevel(logging.INFO)
    console_handler = logging.StreamHandler()
    file_handler = logging.FileHandler("logs/pipeline.log", mode='a', encoding='utf-8')
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s",
                                  datefmt="%Y-%m-%d %H:%M:%S")
    console_handler = setFormatter(formatter)
    file_handler = setFormatter(formatter)
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)


def load_config(config_filepath):

    try:
        with open(config_filepath, 'r') as c
        config = json.load(c)
    except FileNotFoundError:
        logger.info("json file not found")
        sys.exit(1)
    except json.JSONDecodeError:
        logger.info("Error detected in json file")
        sys.exit(1)

    return config

def pull_api(url):

    try:
        response = requests.get(url)



def run_pipeline(config_filepath):

    setup_logging()
    config = load_config(config_filepath)
    users = pull_api(config['users_url'])
    transactions = pull_api(config['transactions_url'])





if __name__ == "__main__":

     if len(argv) != 2:
          print("Missing/Incorrect number of command line arguments")
          sys.exit(1)
     run_pipeline(sys.argv[1])

