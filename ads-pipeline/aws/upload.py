import logging
import os

from credentials import AWS
from tools import SAVE_PATH, cleanup_data

from .s3 import S3

logging.basicConfig(
    format="%(asctime)s:%(levelname)s:%(message)s",
    datefmt="%m/%d/%Y %I:%M:%S p",
    level=logging.INFO,
)


class Upload_S3(object):
    def __init__(self):
        self.s3 = S3(
            aws_bucket=AWS.AWS_BUCKET,
            aws_access_key_id=AWS.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS.AWS_SECRET_ACCESS_KEY,
            aws_region=AWS.AWS_REGION_NAME,
        )

    def upload(self, info: dict):
        try:
            self.name = info.get("ad")
            date = info.get("date")
            if (
                (not date)
                or (not self.name)
                or (
                    self.name not in ["db", "spreadsheet"]
                    and not os.path.exists(f"{SAVE_PATH}/{self.name}")
                )
            ):
                return logging.info(f"[{self.name}] {SAVE_PATH}/{self.name} >> no data save")
            logging.info(f"[{self.name}] {date} >> save start")
            regex = f".gz$"

            return self.__upload(date, regex)
        except Exception as e:
            logging.exception(e)

    def __upload(self, date, regex):
        path = f"{SAVE_PATH}/{self.name}"

        if self.name in ["db", "spreadsheet"]:
            for folder in os.listdir(SAVE_PATH):
                if "." in folder:
                    continue
                save_folder = "mysql" if self.name == "db" else "spreadsheet"
                self.s3.upload_files_by_regex(
                    path=f"{SAVE_PATH}/{folder}", regex=regex, s3_prefix=f"{save_folder}/{folder}"
                )
        else:
            if "mixpanel" in self.name:
                title = "# credentials" if "# credentials" in self.name else "# credentials"
                if "user" in self.name:
                    title = f"user_{title}"
            else:
                title = self.name
            self.s3.upload_files_by_regex(path=path, regex=regex, s3_prefix=title)

        logging.info(f"[{self.name}] {date} >> save done")
        cleanup_data()
