# -*- coding: utf-8 -*-
import json
import logging
from time import sleep

import pandas as pd
import requests
from credentials import ADS
from tools import get_yesterday, save

DATE_FORMAT = "%Y-%m-%d"


logging.basicConfig(
    format="%(asctime)s:%(levelname)s:%(message)s",
    datefmt="%m/%d/%Y %I:%M:%S p",
    level=logging.INFO,
)


class Facebook_ingest(object):
    ad = "facebook"
    __access_token = ADS.FACEBOOK["access_token"]
    __ad_account_id = ADS.FACEBOOK["ad_account_id"]

    def export(self, s=get_yesterday(), u=get_yesterday()):
        self.since, self.until = s, u
        try:
            ### call
            ad = self.ad
            logging.info(f"[{ad}] insight fetch start")
            output = self.get_data()
            logging.info(f"[{ad}] insight fetch success")

            ### save
            return save(ad, output, date=self.until, save_qarquet=True)
        except Exception as e:
            logging.exception(e)
            return None

    def get_data(self):
        url = f"https://graph.facebook.com/v14.0/{self.__ad_account_id}/insights"
        params = {
            "level": "ad",
            "time_range": json.dumps(
                {
                    "since": self.until.strftime(DATE_FORMAT),
                    "until": self.until.strftime(DATE_FORMAT),
                }
            ),
            "fields": json.dumps(
                [
                    "campaign_id",
                    "campaign_name",
                    "adset_id",
                    "adset_name",
                    "ad_id",
                    "ad_name",
                    "spend",
                    "impressions",
                    "clicks",
                    "reach",
                    "cpm",
                    "ctr",
                    "cpp",
                ]
            ),
            "access_token": self.__access_token,
        }
        res = requests.get(url, params=params)
        res.raise_for_status()

        output = []

        if data := res.json().get("data"):
            output.extend(data)

        while next := res.json().get("paging", {}).get("next"):
            res = requests.get(next)
            res.raise_for_status()

            if data := res.json().get("data"):
                output.extend(data)
            else:
                break
            sleep(1)

        df = pd.DataFrame(output)

        df.drop(columns=["date_start", "date_stop"], inplace=True)
        return df
