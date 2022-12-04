import logging
import os
import re
from io import BytesIO
from typing import List, Optional

import boto3
import botocore
import pandas as pd
from credentials import AWS

logging.basicConfig(
    format="%(asctime)s:%(levelname)s:%(message)s",
    datefmt="%m/%d/%Y %I:%M:%S p",
    level=logging.INFO,
)


class S3Bucket:
    def __init__(
        self,
        aws_bucket: Optional[str] = AWS.AWS_BUCKET_RAW,
        aws_access_key_id: Optional[str] = AWS.AWS_ACCESS_KEY_ID,
        aws_secret_access_key: Optional[str] = AWS.AWS_SECRET_ACCESS_KEY,
        aws_region: Optional[str] = AWS.AWS_REGION_NAME,
    ):
        self.__s3 = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region,
        )
        self.__region = aws_region
        self.bucket = aws_bucket

    def set_bucket(self, bucket: str, create: Optional[bool] = False):
        if create:
            try:
                if self.__region is None:
                    self.__region = "us-west-2"
                self.__s3.create_bucket(
                    Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": self.__region}
                )
                self.bucket = bucket
            except self.__s3.exceptions.BucketAlreadyExists:
                self.bucket = bucket
            except self.__s3.exceptions.BucketAlreadyOwnedByYou:
                self.bucket = bucket
        else:
            try:
                self.__s3.head_bucket(Bucket=bucket)
                self.bucket = bucket
            except botocore.exceptions.ClientError:
                pass

    def list_objects_by_regex(self, regex: str):
        next_continuation_token = None
        output = []
        while True:
            try:
                if next_continuation_token is None:
                    result = self.__s3.list_objects_v2(Bucket=self.bucket)
                else:
                    result = self.__s3.list_objects_v2(
                        Bucket=self.bucket, ContinuationToken=next_continuation_token
                    )
                output += [
                    x["Key"]
                    for x in result["Contents"]
                    if x["Key"][-1] != "/" and re.match(regex, x["Key"]) is not None
                ]

                next_continuation_token = result["NextContinuationToken"]
            except KeyError:
                break

        return output

    def get_object(self, key: str, selected_cols: Optional[List[str]] = None):
        data = None
        file, ext = os.path.splitext(key)
        if ext == ".gz":
            compression = "gzip"
        elif ext in [".bz2", ".zip", ".xz"]:
            compression = ext[1:]
        else:
            compression = "infer"

        file, ext = os.path.splitext(file)
        if ext == ".parquet":
            data = pd.read_parquet(
                BytesIO(self.__s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()),
                engine="pyarrow",
                columns=selected_cols,
            )

        elif ext == ".json":
            data = pd.read_json(
                self.__s3.get_object(Bucket=self.bucket, Key=key)["Body"],
                compression=compression,
                lines=True,
            )
        else:
            data = pd.read_csv(
                self.__s3.get_object(Bucket=self.bucket, Key=key)["Body"],
                compression=compression,
                dtype="unicode",
            )
        logging.info(f"S3: {key} Loaded")
        return data

    def get_objects_by_regex(self, regex: str, selected_cols: Optional[List[str]] = None):
        files = self.list_objects_by_regex(regex)

        output = []
        for file in files:
            data = self.get_object(file, selected_cols)

            selected_cols = [col for col in data.columns if col in selected_cols]
            if selected_cols is not None:
                data = data[selected_cols]
            output.append(data)

        if not output:
            return pd.DataFrame()

        output = pd.concat(output)
        return output

    def get_objects_by_regex_by_period(
        self, filename: str, start: str, end: str, selected_cols: Optional[List[str]] = None
    ):
        files = self.list_objects_by_regex(f"{filename}/date=(.*)")
        output = []
        for file in files:
            date = file.split("=")[1].split(".")[0]
            if start <= date and date <= end:
                data = self.get_object(file)
                logging.info(f"downloading.... {filename} {date}")
                data["Date"] = date
                if selected_cols is not None:
                    data = data[selected_cols]
                output.append(data)

        if not output:
            return pd.DataFrame()

        output = pd.concat(output)
        return output

    def get_object_lists_by_regex(self, regex: str):
        return self.list_objects_by_regex(regex)

    def download_object(self, key: str, location: Optional[str] = "."):
        self.__s3.download_file(self.bucket, key, os.path.join(location, key.split("/")[-1]))
        logging.info(f"Downloaded: {os.path.join(location, key.split('/')[-1])}")

    def download_objects_by_regex(self, regex: str, location: Optional[str] = "."):
        if not os.path.exists(location):
            os.makedirs(location, exist_ok=True)
        files = self.list_objects_by_regex(regex)
        for file in files:
            self.download_object(file, location=location)

    def upload_object(self, file: str, prefix: Optional[str] = ""):
        self.__s3.upload_file(file, self.bucket, os.path.join(prefix, file.split("/")[-1]))
        logging.info(f"{file} > {self.bucket}/{os.path.join(prefix, file.split('/')[-1])}")

    def upload_objects_by_regex(self, path: str, regex: str, prefix: Optional[str] = ""):
        for file in os.listdir(path):
            if re.match(regex, file):
                self.upload_object(os.path.join(path, file), prefix)
