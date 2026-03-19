import pandas as pd
import numpy as np
import logging

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


def


def run_pipeline():
    
