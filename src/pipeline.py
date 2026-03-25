import pandas as pd
import numpy as np
import logging
import sys
import requests
import json
import os
sys.path.append(os.path.dirname(__file__))
import clean
import transform

logger = logging.getLogger(__name__)

def setup_logging():

    logger.setLevel(logging.INFO)
    console_handler = logging.StreamHandler()
    file_handler = logging.FileHandler("logs/pipeline.log", mode='a', encoding='utf-8')
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s",
                                  datefmt="%Y-%m-%d %H:%M:%S")
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)


def load_config(config_filepath):

    try:
        with open(config_filepath, 'r') as c:
            config = json.load(c)
    except FileNotFoundError:
        logger.error("json file not found")
        sys.exit(1)
    except json.JSONDecodeError:
        logger.error("Error detected in json file")
        sys.exit(1)

    return config

def pull_api(url):

    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.ConnectionError:
        logger.error("Connection error while retrieving %s", url)
        return None
    except requests.exceptions.Timeout:
        logger.error("Timeout while retrieving %s", url)
        return None
    except requests.exceptions.HTTPError as e:
        logger.error("HTTP error while retrieving %s: %s", url, e)
        return None
    except ValueError:
        logger.error("Failed to parse JSON from %s", url)
        return None


def save_outputs(df_clean: pd.DataFrame, df_sum: pd.DataFrame, df_rejected: pd.DataFrame, config: dict) -> bool:

    df_clean = df_clean.copy()
    df_sum = df_sum.copy()
    df_rejected = df_rejected.copy()

    try:
        output_dir = config['output_dir']
    except KeyError as e:
        logger.error("Unable to find output_dir path in config file: %s", e)
        sys.exit(1)

    clean_fp = (f"{output_dir}/customers_clean.parquet")
    sum_fp = (f"{output_dir}/summary.csv")
    rejected_fp = (f"{output_dir}/rejected.json")

    try:
        df_clean.to_parquet(clean_fp, engine='pyarrow', compression='snappy', index=False)
        logger.info("Successfully wrote %s records to filepath %s", len(df_clean), clean_fp)
        ws_parquet = True
    except OSError:
        logger.error("OSError: Write to filepath %s failed", clean_fp)
        ws_parquet = False
    except ValueError:
        logger.error("ValueError: Write to filepath %s failed", clean_fp)
        ws_parquet = False

    try:
        df_sum.to_csv(sum_fp)
        logger.info("Successfully wrote %s records to filepath %s", len(df_sum), sum_fp)
        ws_csv = True
    except OSError:
        logger.error("OSError: Write to filepath %s failed", sum_fp)
        ws_csv = False
    except ValueError:
        logger.error("ValueError: Write to filepath %s failed", sum_fp)
        ws_csv = False

    try:
        df_rejected.to_json(rejected_fp, orient='records', indent=4, date_format='iso')
        logger.info("Successfully wrote %s records to filepath %s", len(df_rejected), rejected_fp)
        ws_json = True
    except OSError:
        logger.error("OSError: Write to filepath %s failed", rejected_fp)
        ws_json = False
    except ValueError:
        logger.error("ValueError: Write to filepath %s failed", rejected_fp)
        ws_json = False

    return ws_parquet, ws_csv, ws_json


def run_pipeline(config_filepath):

    setup_logging()
    config = load_config(config_filepath)
    users = pull_api(config['users_url'])
    if users is None:
        logger.error("Failed to retrieve data from users API - exiting")
        sys.exit(1)

    transactions = pull_api(config['transactions_url'])
    if transactions is None:
        logger.error("Failed to retrieve data from transactions API - exiting")
        sys.exit(1)

    users_clean, users_rejected, transactions_clean = clean.clean(users, transactions)
    df_transformed = transform.transform(users_clean, transactions_clean)
    df_sum = transform.summarise(df_transformed)
    ws_parquet, ws_csv, ws_json = save_outputs(df_transformed, df_sum, users_rejected, config)

    if ws_parquet and ws_csv and ws_json:
        logger.info("Pipeline completed with no errors")
        return True
    else:
        logger.warning("Pipeline completed with errors")
        return False


if __name__ == "__main__":

     if len(sys.argv) != 2:
          print("Missing/Incorrect number of command line arguments")
          sys.exit(1)
     run_pipeline(sys.argv[1])

