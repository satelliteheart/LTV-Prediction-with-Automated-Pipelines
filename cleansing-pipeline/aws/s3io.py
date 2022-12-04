from datetime import datetime, timedelta
from io import BytesIO
from typing import List, Optional

import boto3
import pandas as pd
from credentials import AWS
from tools import REPLACE_VALUE, SELECTED_COLUMNS, get_last_day

PLATFORM = "AWS"  # "LOCAL" | "AWS"


class DataReader:
    def __init__(
        self,
        offset: int,
        aws_access_key_id: Optional[str] = AWS.AWS_ACCESS_KEY_ID,
        aws_secret_access_key: Optional[str] = AWS.AWS_SECRET_ACCESS_KEY,
        aws_region: Optional[str] = AWS.AWS_REGION_NAME,
    ):
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region,
        )

        self.date = get_last_day(offset).strftime("%Y-%m-%d")
        self.data = None

    def load_type_by_date(self, date=None):
        if not date:
            date = self.date

        data = pd.read_parquet(
            BytesIO(self.s3.get_object(Bucket="# credentials", Key="# credentials")["Body"].read()),
            columns=["# credentials"],
        )
        data = data.astype({"# credentials": "float"})
        data = {
            "corp": data.loc[data["# credentials"] == 3, "# credentials"].unique(),
            "brand": data.loc[data["# credentials"] == 1, "# credentials"].unique(),
        }
        return data

    def load_data_by_date(self, date=None):
        if date:
            self.date = date

        if PLATFORM == "LOCAL":
            data = pd.concat(
                [
                    pd.read_parquet("# credentials", engine="pyarrow"),
                    pd.read_parquet("# credentials", engine="pyarrow"),
                ],
                ignore_index=True,
            )
        else:
            data = pd.concat(
                [
                    pd.read_parquet(
                        BytesIO(
                            self.s3.get_object(Bucket="# credentials", Key="# credentials")[
                                "Body"
                            ].read()
                        ),
                        engine="pyarrow",
                    ),
                    pd.read_parquet(
                        BytesIO(
                            self.s3.get_object(Bucket="# credentials", Key=f"# credentials")[
                                "Body"
                            ].read()
                        ),
                        engine="pyarrow",
                    ),
                ],
                ignore_index=True,
            )
        data.replace(REPLACE_VALUE, inplace=True)
        self.data = data[[col for col in SELECTED_COLUMNS if col in data.columns]]
