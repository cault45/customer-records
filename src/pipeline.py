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
