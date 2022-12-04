# -*- coding: utf-8 -*-

import logging

import pandas as pd
from aws import S3, upload
from credentials import AWS
from leaf import DataCleanser, adreport_groupby, cleansing_ad_report
from pandas import DataFrame
from tools import DEFAULT_DATE_FORMAT, cleanup_data, get_last_day

logging.basicConfig(
    format="%(asctime)s:%(levelname)s:%(message)s",
    datefmt="%m/%d/%Y %I:%M:%S p",
    level=logging.INFO,
)


def cleanse(offset: int = 1):
    cleanup_data()
    last_1_days = get_last_day(offset + 0).strftime(DEFAULT_DATE_FORMAT)
    last_7_days = get_last_day(offset + 6).strftime(DEFAULT_DATE_FORMAT)
    last_30_days = get_last_day(offset + 29).strftime(DEFAULT_DATE_FORMAT)
    last_31_days = get_last_day(offset + 30).strftime(DEFAULT_DATE_FORMAT)
    logging.info(f"get data in {last_30_days} {last_7_days} {last_1_days}")
    mixpanel(offset)
    ads(last_31_days, last_30_days, last_7_days, last_1_days)


def mixpanel(offset: int):
    runner = DataCleanser(offset)
    runner.load_data_by_date()
    runner.id_mapping()
    runner.columns_merge()
    runner.get_data_type()
    runner.type_casting()
    runner.drop_useless()
    runner.save_and_upload()


def ads(last_31_days, last_30_days, last_7_days, last_1_days):
    s3bucket = S3(AWS.AWS_BUCKET_RAW)  ## download s3 bucket
    raw_channels = ["facebook", "naver", "google", "adison", "pincrux"]

    df = DataFrame()
    result = []
    for channel in raw_channels:
        logging.info(f"{channel} - {last_31_days} ~ {last_1_days}")  # 어제 일자 레포트 가져옴

        data = s3bucket.get_objects_by_regex_by_period(channel, last_31_days, last_1_days)
        channel_data = cleansing_ad_report(channel=channel, df=data, s3bucket=s3bucket)
        result.append(channel_data)
        if channel_data.empty:
            continue

    df = pd.concat(result, axis=0)

    for start, end in [
        (last_30_days, last_1_days),
        (last_7_days, last_1_days),
        (last_1_days, last_1_days),
    ]:
        logging.info(f"{channel} - {end} ~ {start}")  # 어제 일자 레포트 가져옴
        df2 = df[(df["date"] >= start) & (df["date"] <= end)].copy()
        new_df = adreport_groupby(df2)
        # 3. 저장하기
        upload(new_df, "channel_report", f"date={start}_{end}")


if __name__ == "__main__":
    cleanse()
