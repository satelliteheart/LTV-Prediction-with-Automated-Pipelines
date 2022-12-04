import logging
import os
import pathlib
import shutil
import time

import numpy as np
import pandas as pd
from credentials import AWS
from pandas import DataFrame
from tools import DEFAULT_DATE_FORMAT, cleanup_data, get_last_day

from .s3 import S3Bucket

logging.basicConfig(
    format="%(asctime)s:%(levelname)s:%(message)s",
    datefmt="%m/%d/%Y %I:%M:%S p",
    level=logging.INFO,
)

s3_cleansed = S3Bucket(aws_bucket=AWS.AWS_BUCKET_CLEANSED)
s3_raw = S3Bucket(aws_bucket=AWS.AWS_BUCKET_RAW)


def download_period(folder: str, start: str, end: str, selected_cols: list = None):
    return s3_cleansed.get_objects_by_regex_by_period(
        filename=folder, start=start, end=end, selected_cols=selected_cols
    )


def download_latest(folder: str, selected_cols: list = None, _from: str = "cleansed"):
    offset = 0
    type = folder.split("/")[-1]
    # get latest data
    while True:
        last_day = get_last_day(offset).strftime(DEFAULT_DATE_FORMAT)
        offset += 1
        key = f"date={last_day}.parquet.gz"
        save_file = f"{type}/{key}"
        if not os.path.exists(save_file):
            try:
                pathlib.Path(type).mkdir(parents=True, exist_ok=True)
                data = s3_cleansed.download_object(key=f"{folder}/{key}", location=type)
            except Exception as e:
                print(e)
                continue
        data = pd.read_parquet(save_file, engine="pyarrow", columns=selected_cols)
        if not data.empty:
            break

    return data


def download(file: str, _from: str = "cleansed"):
    if _from == "cleansed":
        s3 = s3_cleansed
    elif _from == "raw":
        s3 = s3_raw

    if ".parquet.gz" not in file:
        file = f"{file}.parquet.gz"
    logging.info(f"{file} download from s3")
    return s3.get_object(key=file)


def upload(data: DataFrame, folder: str, title: str = ""):
    if data.empty:
        logging.info(f"[{title}] -  dataframe is emptys")
        return

    AD_FILE_PATH = f"/{os.getcwd()}/data"
    pathlib.Path(AD_FILE_PATH).mkdir(parents=True, exist_ok=True)

    # cleansing
    data.to_parquet(
        f"{AD_FILE_PATH}/{title}.parquet.gz",
        compression="gzip",
        engine="pyarrow",
        index=False,
    )

    time.sleep(1)
    s3_cleansed.upload_objects_by_regex(path=AD_FILE_PATH, regex="(.*).gz", prefix=folder)
    cleanup_data()
