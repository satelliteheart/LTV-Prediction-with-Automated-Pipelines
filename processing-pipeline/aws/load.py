import logging
import os
import pathlib
import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from credentials import AWS
from pandas import DataFrame
from tools import DEFAULT_DATE_FORMAT, cleanup_data, get_last_day

from .s3 import S3Bucket

s3_raw = S3Bucket(aws_bucket=AWS.AWS_BUCKET_RAW)
s3_cleansed = S3Bucket(aws_bucket=AWS.AWS_BUCKET_CLEANSED)
s3_report = S3Bucket(aws_bucket=AWS.AWS_BUCKET_REPORT)
s3_appsflyer = S3Bucket(aws_bucket=AWS.AWS_BUCKET_APPSFLYER)


def download(folder: str, selected_cols: list = None, _from: str = "raw"):
    offset = 0
    if _from == "raw":
        s3 = s3_raw
    elif _from == "cleansed":
        s3 = s3_cleansed
    # get latest data

    pathlib.Path(folder).mkdir(parents=True, exist_ok=True)
    while True:
        last_day = get_last_day(offset + 1).strftime(DEFAULT_DATE_FORMAT)
        offset += 1

        if not os.path.exists(f"{folder}/date={last_day}.parquet.gz"):
            try:
                logging.info(f"Downloading data from S3 - {folder}/date={last_day}.parquet.gz")
                s3.download_object(key=f"{folder}/date={last_day}.parquet.gz", location=folder)
            except:
                logging.info("File not found")
                continue
        data = pd.read_parquet(
            f"{folder}/date={last_day}.parquet.gz", engine="pyarrow", columns=selected_cols
        )
        if not data.empty:
            break

    return data


def download_specific(folder: str, title: str, _from: str = "raw", selected_cols: list = None):
    if _from == "raw":
        s3 = s3_raw
    elif _from == "cleansed":
        s3 = s3_cleansed
    # get latest data
    return s3.get_objects_by_regex(
        regex=f"{folder}/{title}.parquet.gz", selected_cols=selected_cols
    )


def download_period(
    folder: str, start: str, end: str, selected_cols: list = None, _from: str = "raw"
):
    start_datetime = datetime.strptime(start, DEFAULT_DATE_FORMAT)

    pathlib.Path(folder).mkdir(parents=True, exist_ok=True)

    if _from == "raw":
        s3 = s3_raw
    elif _from == "cleansed":
        s3 = s3_cleansed
    elif _from == "# credentials":
        s3 = s3_appsflyer

    period = 0  # 0 ~ 29
    while True:
        key = (start_datetime + timedelta(days=period)).strftime(DEFAULT_DATE_FORMAT)
        if not os.path.exists(f"{folder}/date={key}.parquet.gz"):
            try:
                s3.download_object(key=f"{folder}/date={key}.parquet.gz", location=folder)
            except:
                logging.info(f"File - {period} `{folder}/date={key}.parquet.gz` not found")
        if key >= end:
            break
        period += 1

    result = []
    for key in os.listdir(folder):
        if key.endswith(".parquet.gz") and (
            key >= f"date={start}.parquet.gz" and key <= f"date={end}.parquet.gz"
        ):
            data = pd.read_parquet(f"{folder}/{key}", engine="pyarrow")
            if selected_cols:
                final_columns = [col for col in selected_cols if col in data.columns]
                data = data[final_columns]
            result.append(data)
    return pd.concat(result)


def download_latest(folder: str, selected_cols: list = None, _from: str = "raw"):
    offset = 0
    type = folder.split("/")[-1]

    if _from == "raw":
        s3 = s3_raw
    elif _from == "cleansed":
        s3 = s3_cleansed
    # get latest data
    while True:
        last_day = get_last_day(offset).strftime(DEFAULT_DATE_FORMAT)
        offset += 1
        key = f"date={last_day}.parquet.gz"
        save_file = f"{type}/{key}"
        if not os.path.exists(save_file):
            try:
                pathlib.Path(type).mkdir(parents=True, exist_ok=True)
                data = s3.download_object(key=f"{folder}/{key}", location=type)
            except Exception as e:
                print(e)
                continue
        data = pd.read_parquet(save_file, engine="pyarrow", columns=selected_cols)
        if not data.empty:
            break

    return data


def download_appsflyer(date: str, folder: str, selected_cols: list = None, _from: str = "maymay"):
    if _from == "# credentials":
        s3 = s3_appsflyer
    output = s3.get_object_lists_by_regex(regex=f"{folder}/(.*){date}(.*).csv.gz")
    if not output:
        return False

    AD_FILE_PATH = f"/{os.getcwd()}/appsflyer"
    pathlib.Path(AD_FILE_PATH).mkdir(parents=True, exist_ok=True)

    for o in output:
        if ("reinstall" in o) or ("reinapp" in o):
            continue
        s3.download_object(key=o, location=AD_FILE_PATH)
    return True


def upload(
    data: DataFrame, folder: str, title: str = "", _to: str = "cleansed", _save_raw: bool = False
):
    AD_FILE_PATH = f"/{os.getcwd()}/data"
    pathlib.Path(AD_FILE_PATH).mkdir(parents=True, exist_ok=True)
    if not title:
        title = f"date={get_last_day(1).strftime(DEFAULT_DATE_FORMAT)}"

    # cleansing
    data.replace({np.nan: None}, inplace=True)

    if not _save_raw:
        data = data.astype(str)
    else:
        data[["# credentails"]].replace({"None": 0, np.nan: 0}, inplace=True)
        data["# credentails"].replace({None: False}, inplace=True)
        data[["# credentails"]].replace({"": None}, inplace=True)
        data = data.astype(
            {
                "# credentails": "float64",
                "# credentails": "bool",
                "# credentails": "bool",
                "# credentails": "float64",
                "# credentails": "bool",
                "# credentails": "float64",
                "# credentails": "float64",
                "# credentails": "float64",
            }
        ).replace({"None": None, np.nan: None})

    data.to_parquet(
        f"{AD_FILE_PATH}/{title}.parquet.gz",
        compression="gzip",
        engine="pyarrow",
        index=False,
    )
    time.sleep(1)

    if _to == "raw":
        s3 = s3_raw
    elif _to == "cleansed":
        s3 = s3_cleansed
    elif _to == "report":
        s3 = s3_report

    s3.upload_objects_by_regex(path=AD_FILE_PATH, regex="(.*).gz", prefix=folder)
    cleanup_data()
