import logging

import numpy as np
import pandas as pd
from aws import download, download_latest, upload
from tools import *

logging.basicConfig(
    format="%(asctime)s:%(levelname)s:%(message)s",
    datefmt="%m/%d/%Y %I:%M:%S p",
    level=logging.INFO,
)


def processing(offset: int = 1):
    folder_name = "accumulated_ltv"

    today = get_last_day(offset).strftime(DEFAULT_DATE_FORMAT)
    yesterday = get_last_day(offset + 1).strftime(DEFAULT_DATE_FORMAT)
    logging.info(f"today is {today}")

    logging.info(f"try to fetch data from {yesterday}")
    final_ltv = pd.DataFrame(
        {
            "date": [],
            "distinct_id": [],
            "brand_challenge_count": [],
            "brand_challenge_value": [],
            "_brand_challenge_value": [],
            "challenge_count": [],
            "challenge_value": [],
            "_challenge_value": [],
            "store_value": [],
        }
    )
    if today > "2022-07-27":
        try:
            final_ltv = download(f"{folder_name}/date={yesterday}", _from="cleansed")
        except Exception as e:
            logging.info("no file", e)

    logging.info(f"apply astype -> yesterday data")
    final_ltv.drop(columns=["date"], inplace=True)
    final_ltv = final_ltv.astype(
        {
            "distinct_id": "string",
            "brand_challenge_count": "int64",
            "brand_challenge_value": "float64",
            "_brand_challenge_value": "float64",
            "challenge_count": "int64",
            "challenge_value": "float64",
            "_challenge_value": "float64",
            "store_value": "float64",
        }
    )

    print(final_ltv.head(10))

    logging.info("start fetching data")
    day_df = download(f"mixpanel_report/date={today}")

    logging.info(f"concatenating {today} data")
    df = pd.concat(
        [process_challengers(day_df), process_stores(day_df), final_ltv], axis=0
    ).replace({np.nan: 0, None: 0})

    logging.info(f"group by distinct_id")
    df = check_device_id(df)
    df = df.groupby(["distinct_id"]).sum(numeric_only=True).reset_index()

    logging.info(f"upload final ltv. title : `date={today}.parquet.gz`")
    df["date"] = today
    upload(data=df, folder=folder_name, title=f"date={today}")


def check_device_id(df: pd.DataFrame):
    user_info = download_latest(folder="user_info", _from="cleansed")
    device_ids = df[df["distinct_id"].str.contains("-")]["distinct_id"].tolist()

    for device_id in device_ids:
        exists = user_info.query(f"device_id == '{device_id}' ").head(1)
        if not exists.empty:
            df.loc[df["distinct_id"] == device_id, "distinct_id"] = exists["distinct_id"].tolist()[
                0
            ]
    return df


def process_challengers(day_df: pd.DataFrame):
    BRAND_CHALLENGE_COMPLETE_CHECKOUT = "brand_challenge_complete_checkout"
    CHALLENGE_COMPLETE_CHECKOUT = "challenge_complete_checkout"
    challengers_events = [BRAND_CHALLENGE_COMPLETE_CHECKOUT, CHALLENGE_COMPLETE_CHECKOUT]

    challengers_df = day_df[day_df["event"].isin(challengers_events)]
    if len(challengers_df) == 0:
        return pd.DataFrame(
            {
                "distinct_id": [],
                "brand_challenge_count": [],
                "brand_challenge_value": [],
                "_brand_challenge_value": [],
                "challenge_count": [],
                "challenge_value": [],
                "_challenge_value": [],
            }
        )

    challengers_df.loc[
        challengers_df["event"] == BRAND_CHALLENGE_COMPLETE_CHECKOUT, "brand_challenge_value"
    ] = 0  # credencials

    challengers_df.loc[
        challengers_df["event"] == BRAND_CHALLENGE_COMPLETE_CHECKOUT, "brand_challenge_count"
    ] = 0  # credencials

    challengers_df.loc[
        challengers_df["event"] == CHALLENGE_COMPLETE_CHECKOUT, "_challenge_value"
    ] = 0  # credencials

    challengers_df.loc[
        challengers_df["event"] == CHALLENGE_COMPLETE_CHECKOUT, "challenge_count"
    ] = 0  # credencials

    return (
        challengers_df[
            [
                "distinct_id",
                "brand_challenge_value",
                "revenue_from_brand",
                "revenue_from_challenge",
                "_challenge_value",
                "brand_challenge_count",
                "challenge_count",
            ]
        ]
        .replace({None: 0, np.nan: 0})
        .rename(
            columns={
                "revenue_from_brand": "_brand_challenge_value",
                "revenue_from_challenge": "challenge_value",
            }
        )
    )


def process_stores(day_df: pd.DataFrame):

    ## 2. stores
    stores_events = ["store_complete_checkout"]
    stores_df = day_df[day_df["event"].isin(stores_events)]
    if len(stores_df) == 0:
        return pd.DataFrame({"distinct_id": [], "store_value": []})

    return (
        stores_df[["distinct_id", "revenue"]]
        .replace({None: 0, np.nan: 0})
        .rename(
            columns={
                "revenue": "store_value",
            }
        )
    )


if __name__ == "__main__":
    processing()
