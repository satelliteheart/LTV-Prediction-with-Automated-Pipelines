# -*- coding: utf-8 -*-

import json
import os
import pathlib
import re
import shutil
from datetime import datetime, timedelta, timezone

from pandas import DataFrame

SAVE_PATH = f"/{os.getcwd()}/data"

KST_YESTERDAY = timezone(timedelta(hours=-15))
KST_TODAY = timezone(timedelta(hours=+9))

DEFAULT_DATE_FORMAT = "%Y-%m-%d"


get_last_day = lambda chunk=0: (datetime.now(KST_TODAY) - timedelta(days=chunk))
TODAY = get_last_day(0).strftime(DEFAULT_DATE_FORMAT)


def cleanup_data():
    try:
        shutil.rmtree(SAVE_PATH)
    except:
        pass
    pathlib.Path(SAVE_PATH).mkdir(parents=True, exist_ok=True)


def get_cell_data(data: DataFrame, col: str):
    row = data.index[0]
    return data.loc[row][col]


def get_diff_datetime(start, end, format=DEFAULT_DATE_FORMAT):
    return (datetime.strptime(end, format) - datetime.strptime(start, format)).days


def is_float(value):
    try:
        float(value)
        return True
    except:
        return False


def expand_properties(v: str, columns_to_save: list):
    if isinstance(v, str):
        v: dict = json.loads(v)
    copy_v = v.copy()
    for key in copy_v.keys():
        if key not in columns_to_save:
            v.pop(key, False)
    return v


def check_os(values):
    try:
        for value in values:
            if str(value).lower() == "nan":
                continue
            elif value == None:
                continue
            try:
                v = value.lower()
                if "android" in v or "samsung" in v:
                    return "aos"
                elif "ios" in v or "ipad" in v or "mac" in v or "iphone" in v or "apple" in v:
                    return "ios"
                elif "browser" in v and ("mobile" not in v):
                    return "web"
            except:
                continue
    except:
        return None


def cleanse_number_types(v):
    try:
        if isinstance(v, str):
            if "#DIV/0!" in v:
                return 0.0
            elif "#REF!" in v:
                return 0.0
            return float(re.sub(r"[^0-9.]", "", v))
        else:
            return float(v)
    except:
        return 0.0


def change_challenge_id_type(v):
    try:
        if isinstance(v, list):
            return str(v[0]) if len(v) == 1 else ",".join(v)
        elif not v or v == "":
            return ""
        else:
            return str(v)
    except:
        return ""
