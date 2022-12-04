# -*- coding: utf-8 -*-
import json
import logging
from datetime import datetime, timedelta

import boto3
import credentials.aws_auth as AUTH
from dateutil.relativedelta import relativedelta
from tools import KST_YESTERDAY, get_yesterday

logging.basicConfig(
    format="%(asctime)s:%(levelname)s:%(message)s",
    datefmt="%Y-%m-%d %I:%M:%S %p",
    level=logging.INFO,
)
datetime.now(KST_YESTERDAY) - relativedelta(months=6)


class Appsflyer_ingest(object):
    def __init__(self) -> None:
        self.__lambda = boto3.client(
            "lambda",
            aws_access_key_id=AUTH.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AUTH.AWS_SECRET_ACCESS_KEY,
            region_name=AUTH.AWS_REGION_NAME,
        )

    def export(self):
        REPORT_TYPE = [
            # credentials
        ]

        for report_type in REPORT_TYPE:
            day = 0

            payload = {
                "client": "",  # credentials
                "start_date": (get_yesterday() - timedelta(days=day)).strftime("%Y-%m-%d"),
                "end_date": (get_yesterday() - timedelta(days=day)).strftime("%Y-%m-%d"),
                "report_type": report_type,
            }

            try:
                self.__lambda.invoke(
                    FunctionName=AUTH.AWS_APPSFLYER_LAMBDA,
                    InvocationType="Event",
                    Payload=json.dumps({"queryStringParameters": payload}),
                )
            except Exception as e:
                logging.exception("Appsflyer export error, " + str(e))
