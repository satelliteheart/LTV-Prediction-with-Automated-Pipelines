import json
import os
import pathlib
import shutil
from datetime import datetime, timedelta, timezone

import pandas as pd

SAVE_PATH = f"/{os.getcwd()}/data"

KST_YESTERDAY = timezone(timedelta(hours=-15))
KST_TODAY = timezone(timedelta(hours=+9))

DEFAULT_DATE_FORMAT = "%Y-%m-%d"

get_last_day = lambda chunk=0: (datetime.now(KST_TODAY) - timedelta(days=chunk))
TODAY = get_last_day(0).strftime(DEFAULT_DATE_FORMAT)


def cleanup_data():
    for delete_path in [
        SAVE_PATH,
        # credentails
    ]:
        try:
            shutil.rmtree(delete_path)
        except:
            pass

    pathlib.Path(SAVE_PATH).mkdir(parents=True, exist_ok=True)


def get_df_by_key(df: pd.DataFrame, key: str, key_id: int):
    try:
        df = df[df[key] == key_id]
        df = df.reset_index().iloc[0].to_dict()
        df.pop("index", False)
        return df
    except Exception as e:
        print(e)
        return {}


def str_to_float(v: str):

    try:
        return float(v)
    except:
        return None
