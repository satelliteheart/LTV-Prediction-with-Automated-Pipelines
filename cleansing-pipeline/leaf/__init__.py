import logging

from aws import S3
from pandas import DataFrame

from .adchannels import adison, facebook, google, naver, pincrux
from .mixpanel import DataCleanser


def cleansing_ad_report(channel: str, df: DataFrame, s3bucket: S3) -> DataFrame:
    logging.info(f"start cleansing process adchannels - {channel}")
    if df.empty:
        return df

    if channel == "facebook":
        df = facebook(df)
    elif channel == "google":
        df = google(df)
    elif channel == "naver":
        df = naver(df)
    elif channel == "adison":
        df = adison(df)
    elif channel == "pincrux":
        df = pincrux(df, s3bucket)

    # reset index
    try:
        df.reset_index(inplace=True)
    except:
        pass

    # drop columns
    if "Unnamed: 0" in df.columns:
        df.drop(columns=["Unnamed: 0"], inplace=True)
    if "index" in df.columns:
        df.drop(columns=["index"], inplace=True)
    return df
