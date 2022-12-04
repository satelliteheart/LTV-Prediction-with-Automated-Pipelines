import os
import pathlib
import shutil
from datetime import datetime, timedelta, timezone

SAVE_PATH = f"/{os.getcwd()}/data"

KST_YESTERDAY = timezone(timedelta(hours=-15))
KST_TODAY = timezone(timedelta(hours=+9))

DEFAULT_DATE_FORMAT = "%Y-%m-%d"

get_yesterday = lambda chunk=0: (datetime.now(KST_YESTERDAY) - timedelta(days=chunk))
get_last_day = lambda chunk=0: (datetime.now(KST_TODAY) - timedelta(days=chunk))


TODAY = get_last_day(0).strftime(DEFAULT_DATE_FORMAT)


def cleanup_data():
    try:
        shutil.rmtree(SAVE_PATH)
    except:
        pass
    pathlib.Path(SAVE_PATH).mkdir(parents=True, exist_ok=True)
