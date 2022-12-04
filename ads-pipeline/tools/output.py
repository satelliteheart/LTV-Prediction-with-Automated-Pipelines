import logging
import os
import pathlib
from datetime import datetime
from time import sleep

import numpy as np
from pandas import DataFrame

DATE_FORMAT = "%Y-%m-%d"


logging.basicConfig(
    format="%(asctime)s:%(levelname)s:%(message)s",
    datefmt="%m/%d/%Y %I:%M:%S p",
    level=logging.INFO,
)


def save(
    ad: str,
    output: DataFrame or bytes,
    date: datetime,
    is_df=False,
    save_qarquet=False,
) -> dict:
    ### save
    date = date.strftime(DATE_FORMAT)

    if "spreadsheet" in ad:
        split_chunks = ad.split("::")
        sheet_name = split_chunks[1] if len(split_chunks) == 2 else "-".join(split_chunks[1:])
        if not sheet_name:
            sheet_name = "Sheet1"
        ad = sheet_name.replace(" ", "_")

    ## 파일 경로 만들기
    AD_FILE_PATH = f"/{os.getcwd()}/data/{ad}"
    AD_FILE_NAME = f"{AD_FILE_PATH}/date={date}"
    pathlib.Path(AD_FILE_PATH).mkdir(parents=True, exist_ok=True)

    # 비어있다면 종료
    if isinstance(output, DataFrame) and output.empty:
        logging.info(f"{AD_FILE_NAME}'s data is empty")
        return {}

    # nan type 제거
    try:
        logging.info(f"apply nan -> None")
        output.replace({np.nan: None}, inplace=True)
    except:
        pass

    if is_df:  # adison, naver
        output.fillna(0).to_csv(AD_FILE_NAME + ".csv.gz", sep=",", compression="gzip")

    elif save_qarquet:  # facebook
        try:
            logging.info(f"apply astype")
            output = output.astype(str)
        except Exception as e:
            print(str(e))

        # 파일 이름 settings 및 저장
        output.to_parquet(AD_FILE_NAME + ".parquet.gz", engine="pyarrow", compression="gzip")

    sleep(1)
    return {"date": f"date={date}", "ad": ad}
