import logging
import os
import pathlib
import time

import numpy as np
from credentials import AWS
from pandas import DataFrame
from tools import DEFAULT_DATE_FORMAT, cleanup_data, get_last_day

from .s3 import S3Bucket

s3_download = S3Bucket(aws_bucket=AWS.AWS_BUCKET_RAW)
s3_upload = S3Bucket(aws_bucket=AWS.AWS_BUCKET_CLEANSED)


def download(folder: str, selected_cols: list = None):
    """
    Download data from S3 which is latest data in the folder
    Args:
        folder (str)                    : folder name in S3
        selected_cols (list, optional)  : Get specific columns in data.
                                          Defaults to None.
                                          If None, all columns will be downloaded.
    Returns:
        pd.DataFrame: Latest data in S3 folder
    """
    offset = 0
    # get latest data
    while True:
        last_day = get_last_day(offset).strftime(DEFAULT_DATE_FORMAT)
        offset += 1
        data = s3_download.get_objects_by_regex(
            regex=f"{folder}/date={last_day}.parquet.gz", selected_cols=selected_cols
        )
        if not data.empty:
            break

    return data


def upload(data: DataFrame, folder: str, title: str = "", offset: int = 1):
    if data.empty:
        logging.info(f"[{title}] -  dataframe is empty")
        return

    AD_FILE_PATH = f"/{os.getcwd()}/data"
    pathlib.Path(AD_FILE_PATH).mkdir(parents=True, exist_ok=True)

    if not title:
        title = f"date={get_last_day(offset).strftime(DEFAULT_DATE_FORMAT)}"

    # cleansing
    data.replace({np.nan: None}, inplace=True)
    data = data.astype(str)
    data.to_parquet(
        f"{AD_FILE_PATH}/{title}.parquet.gz",
        compression="gzip",
        engine="pyarrow",
        index=False,
    )

    time.sleep(1)
    s3_upload.upload_objects_by_regex(path=AD_FILE_PATH, regex="(.*).gz", prefix=folder)
    cleanup_data()
