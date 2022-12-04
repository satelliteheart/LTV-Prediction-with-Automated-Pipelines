# -*- coding: utf-8 -*-

import logging
from itertools import product

import pandas as pd
import requests
from bs4 import BeautifulSoup
from credentials import ADS
from tools import get_last_day, save

DATE_FORMAT = "%Y%m%d"


logging.basicConfig(
    format="%(asctime)s:%(levelname)s:%(message)s",
    datefmt="%m/%d/%Y %I:%M:%S p",
    level=logging.INFO,
)


class Adison_ingest(object):
    ad = "adison"
    ### auth
    __campaign_id = ADS.ADISON["campaign_id"]
    __access_token = ADS.ADISON["access_token"]

    def __init__(self):
        ### params
        self.url = "https://ao.adison.co/admin/reports"
        self.params = {
            "campaign_id": self.__campaign_id,
            "date_unit": "day",
            "access_token": self.__access_token,
            "is_partner": "",
        }
        self.output = []

        self.report_group = {
            # credentials
        }
        self.platform = {
            # credentials
        }

        self.headers = {
            "authority": "ao.adison.co",
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "accept-language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            "cache-control": "max-age=0",
            "sec-ch-ua": '"Chromium";v="104", " Not A;Brand";v="99", "Google Chrome";v="104"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36",
        }

    def export(self, s=get_last_day(), u=get_last_day()):
        self.since, self.until = s, u
        try:
            ### call
            ad = self.ad
            logging.info(f"[{ad}] insight fetch start")
            output = self.get_data()
            logging.info(f"[{ad}] insight fetch success")

            ### save
            return save(ad, output, date=self.until, is_df=True)
        except:
            return None

    def get_data(self):
        key_list = iter(product(*[self.report_group.keys(), self.platform.keys()]))

        for key in key_list:
            report_key, platform_key = key
            params = {
                **self.params,
                "report_group[]": report_key,
                "platform[]": platform_key,
            }

            res = requests.get(url=self.url, params=params, headers=self.headers)
            self._cleansing_data(res.text, report_key=report_key, platform_key=platform_key)

        return pd.DataFrame(self.output[1:], columns=self.output[0])

    def _get_header(self, res: str):
        soup = BeautifulSoup(res, "lxml")
        ths = soup.select(
            "body > div > div.main-content > div > div:nth-child(2) > div > div > div.table-responsive.mb-0 > table > thead > tr > th"
        )
        headers = ["채널", "플랫폼", "캠페인"] + [th.get_text().strip() for th in iter(ths)]

        self.campaign = (
            soup.select_one(
                "body > div > div.main-content > div > div.row.justify-content-center > div > div > div > div > div > h1"
            )
            .get_text()
            .strip()
        )

        return self.output.append(headers)

    def _cleansing_data(self, res: str, *arg, **kwargs):
        soup = BeautifulSoup(res, "lxml")
        trs = soup.select(
            "body > div > div.main-content > div > div:nth-child(2) > div > div > div.table-responsive.mb-0 > table > tbody > tr"
        )
        if not self.output:
            self._get_header(res)

        for tr in iter(trs):
            if self.until.strftime(DATE_FORMAT) not in tr.get_text():
                continue

            if "Total" in tr.get_text():
                continue

            row = [
                self.report_group[kwargs.get("report_key")],
                self.platform[kwargs.get("platform_key")],
                self.campaign,
            ] + [td.get_text().strip() for td in iter(tr.select("td"))]

            return self.output.append(row)
