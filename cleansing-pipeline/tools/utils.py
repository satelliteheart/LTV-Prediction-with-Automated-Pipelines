# -*- coding: utf-8 -*-

import json
import logging
import os
import pathlib
import re
import shutil
from datetime import datetime, timedelta, timezone

logging.basicConfig(
    format="%(asctime)s:%(levelname)s:%(message)s",
    datefmt="%m/%d/%Y %I:%M:%S p",
    level=logging.INFO,
)


SAVE_PATH = f"/{os.getcwd()}/data"

KST_YESTERDAY = timezone(timedelta(hours=-15))
KST_TODAY = timezone(timedelta(hours=+9))
DEFAULT_DATE_FORMAT = "%Y-%m-%d"


get_last_day = lambda chunk=0: (datetime.now(KST_TODAY) - timedelta(days=chunk))

TODAY = get_last_day().strftime(DEFAULT_DATE_FORMAT)


def cleanup_data():
    try:
        shutil.rmtree(SAVE_PATH)
    except:
        pass
    pathlib.Path(SAVE_PATH).mkdir(parents=True, exist_ok=True)


def cleanse_number_types(v):
    try:
        if isinstance(v, str):
            return float(re.sub(r"[^0-9.]", "", v))
        else:
            return float(v)
    except Exception as e:
        return None


def string_to_int(value):
    try:
        value = json.loads(value)
        if isinstance(value, int) or isinstance(value, float):
            return int(value)
        return None
    except:
        if value:
            print(value)
        return None


def string_to_float(value):
    try:
        return float(value)
    except:
        if value:
            print(value)
        return None


def json_decoding(string):
    try:
        return json.loads(string)
    except Exception:
        if isinstance(string, str):
            print(string)
        return None


def check_if_uuid_type(idstr):
    return isinstance(idstr, str) and "-" in idstr


def check_if_custom_id_type(idstr):
    return isinstance(idstr, str) and "-" not in idstr


def is_true(value):
    return value == "true" or value == "True" or value == True
