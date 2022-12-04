# -*- coding: utf-8 -*-
import logging
import os
import re
from typing import List, Optional

import boto3
import botocore
import pandas as pd

logging.basicConfig(
    format="%(asctime)s:%(levelname)s:%(message)s",
    datefmt="%Y-%m-%d %I:%M:%S %p",
    level=logging.INFO,
)


class S3(object):
    def __init__(
        self, aws_bucket: str, aws_access_key_id: str, aws_secret_access_key: str, aws_region: str
    ) -> None:

        """
        Parameter
            * aws_bucket[string, Required]: Athena table query result base bucket
            * aws_access_key_id[string, Required]: AWS IAM aws access key id
            * aws_secret_access_key[string, Required]: AWS IAM aws secret access key
            * aws_region[string, Required]: Athena table region
        """
        self.__s3 = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region,
        )

        self.__bucket = None
        self.set_bucket(aws_bucket)

    def set_bucket(self, bucket) -> None:
        """
        Set Bucket

        Parameter
            * bucket[string, Required]: Name of bucket
        """
        try:
            self.__s3.head_bucket(Bucket=bucket)
            self.__bucket = bucket
        except botocore.exceptions.ClientError:
            logging.warning(
                f"S3 Initialization: There is no bucket named {bucket}. Create bucket, or check if the name is correct"
            )
            return None

    def create_bucket(self, bucket: str, region: Optional[str] = None) -> None:

        """
        Create S3 Bucket

        Parameter
            * bucket[string, Required]: Create Bucket
            * region[string, Optional]: AWS Region of Bucket
        """
        try:
            if region is None:
                self.__s3.create_bucket(
                    Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": region}
                )
            else:
                self.__s3.create_bucket(Bucket=bucket)

            logging.info(f"S3: Bucket s3://{bucket} is created")
        except botocore.exception.ClientError as e:
            logging.warning(f"S3: Create Bucket: {e}")

        self.__bucket = bucket

    def list_objects(
        self, prefix: Optional[str] = "", suffix: Optional[str] = "", regex: Optional[str] = ""
    ) -> List[str]:
        """
        List objects in Bucket

        Parameter
            * prefix[string, Optional]: Prefix of Objects
            * suffix[string, Optional]: Suffix of Objects
            * regex[string, Optional]: Regex Pattern of Objects
        """
        NextContinuationToken = None
        flist = []
        while True:
            try:
                if NextContinuationToken is None:
                    result = self.__s3.list_objects_v2(Bucket=self.__bucket, Prefix=prefix)
                else:
                    result = self.__s3.list_objects_v2(
                        Bucket=self.__bucket, Prefix=prefix, ContinuationToken=NextContinuationToken
                    )

                flist += [
                    x["Key"]
                    for x in result["Contents"]
                    if x["Key"][-1] != "/"
                    and x["Key"].split("/")[-1].find(suffix) != -1
                    and re.search(regex, x["Key"]) is not None
                ]

                NextContinuationToken = result["NextContinuationToken"]

            except KeyError:
                break
            except TypeError:
                logging.warning(f"S3.list_objects: Bucket is not set.")
                break

        if len(flist) == 0:
            logging.warning("S3: List Objects: Return empty list")
        else:
            logging.info(f"S3: List Objects: Find {len(flist)} object(s)")

        return flist

    def download_objects(
        self,
        path: Optional[str] = ".",
        prefix: Optional[str] = "",
        suffix: Optional[str] = "",
        regex: Optional[str] = "",
    ) -> List[str]:
        """
        Download objects in Bucket

        Parameter
            * path[string, Optional]: Path to Save Objects
            * prefix[string, Optional]: Prefix of Objects
            * suffix[string, Optional]: Suffix of Objects
            * regex[string, Optional]: Regex Pattern of Objects
        """
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)

        flist = self.list_objects(prefix=prefix, suffix=suffix, regex=regex)
        for key in flist:
            logging.info(f"s3://{self.__bucket}/{key} > {os.path.join(path, key.split('/')[-1])}")
            self.__s3.download_file(self.__bucket, key, os.path.join(path, key.split("/")[-1]))

    def load_object_to_dataframe(
        self,
        prefix: Optional[str] = "",
        suffix: Optional[str] = "",
        regex: Optional[str] = "",
        selected_cols: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        Load objects in Bucket as Pandas Dataframe

        Parameter
            * prefix[string, Optional]: Prefix of Objects
            * suffix[string, Optional]: Suffix of Objects
            * regex[string, Optional]: Regex Pattern of Objects
            * selected_cols[List[str], Optional]: Columns to load
        """

        keys = self.list_objects(prefix=prefix, suffix=suffix, regex=regex)

        output = []
        for key in keys:
            suffix = os.path.splitext(key)[1]
            if suffix.find("gz") != -1:
                infer = "gzip"
                suffix = os.path.splitext(os.path.splitext(key)[0])[1]
            else:
                infer = "infer"

            if suffix.find("json") != -1:
                buf = pd.read_json(
                    self.__s3.get_object(Bucket=self.__bucket, Key=key)["Body"],
                    lines=True,
                    compression=infer,
                )
            else:
                buf = pd.read_csv(
                    self.__s3.get_object(Bucket=self.__bucket, Key=key)["Body"],
                    dtype="unicode",
                    compression=infer,
                )

            if selected_cols != None:
                buf = buf[selected_cols]
            output.append(buf)
            logging.info(f"s3://{self.__bucket}/{key} Loaded")

        output = pd.concat(output).drop_duplicates()
        return output

    def upload_files_by_regex(self, path: str, regex: str, s3_prefix: Optional[str] = "") -> None:
        """
        Upload files to Bucket

        Parameter
            * path[string, Required]: File Path
            * regex[string, Required]: Regex Pattern of Target File Name
            * s3_prefix[string, Optional]: Folder prefix to upload file
        """
        for file in os.listdir(path):
            if re.search(regex, file) is not None:
                self.__s3.upload_file(
                    os.path.join(path, file), self.__bucket, os.path.join(s3_prefix, file)
                )
                logging.info(f"{path}/{file} > s3://{self.__bucket}/{s3_prefix}/{file}")
