import requests
import pandas as pd
import os
from datetime import datetime

DATA_LAKE_DIR = os.environ.get(
    "ORDER_DATA_LAKE_DIR"
    "/opt/airflow/data_lake/orders"
)