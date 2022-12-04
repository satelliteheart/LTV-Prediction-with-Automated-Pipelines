# -*- coding: utf-8 -*-
import json
import logging
import time

import pandas as pd
import requests
from credentials import ADS
from tools import Signature, get_yesterday, save

DATE_FORMAT = "%Y-%m-%d"


logging.basicConfig(
    format="%(asctime)s:%(levelname)s:%(message)s",
    datefmt="%m/%d/%Y %I:%M:%S p",
    level=logging.INFO,
)


class Naver_ingest(object):
    ad = "naver"
    ### auth
    __naver_api_key = ADS.NAVER["api_key"]
    __secret_key = ADS.NAVER["secret_key"]
    __customer_id = str(ADS.NAVER["custom_id"])

    def __init__(self):
        self.BASE_URL = "https://api.naver.com"
        self.fields = [
            "clkCnt",  # 클릭수
            "impCnt",  # 노출수
            "salesAmt",  # 총비용
            "ctr",  # 클릭률
            "cpc",  # 평균클릭비용
            "ccnt",  # 전환수
            "crto",  # 전환율
            "convAmt",  # 전환비용
            "viewCnt",  # 노출수
            "ror",  # 광고수익률 (전환매출/총비용)
            "cpConv",  # 전환당비용 (총비용/전환수)
        ]

    def __get_header(self, method, uri):
        timestamp = str(round(time.time() * 1000))
        signature = Signature.generate(timestamp, method, uri, self.__secret_key)
        return {
            "Content-Type": "application/json; charset=UTF-8",
            "X-Timestamp": timestamp,
            "X-API-KEY": self.__naver_api_key,
            "X-Customer": self.__customer_id,
            "X-Signature": signature,
        }

    def export(self, s=get_yesterday(), u=get_yesterday()):
        self.since, self.until = s, u
        try:
            ad = self.ad
            ### call
            logging.info(f"[{ad}] insight fetch start")
            output = self.get_data()
            logging.info(f"[{ad}] insight fetch success")

            ### save
            return save(ad, output, date=self.until, is_df=True)
        except Exception as e:
            logging.exception(e)
            return None

    def get_data(self):
        campaign_ids = self.get_campaign_ids()

        # 캠페인 정보 알아내기
        campaign_list = []
        campaign_check_list = [
            "nccCampaignId",
            "name",
            "campaignTp",
            "totalChargeCost",
            "status",
            "expectCost",
        ]
        # get only active campaigns
        for campaign in campaign_ids:
            new_campaign_data = {}
            for k, v in campaign.items():
                if k in campaign_check_list:  # and (campaign["status"] == "ELIGIBLE"):
                    new_campaign_data.update({"campaign_" + k: v})
            if not new_campaign_data:
                continue
            campaign_list.append(new_campaign_data)

        # campaign_info
        campaign_stat_list = self.get_stats(
            [campaign.get("campaign_nccCampaignId") for campaign in campaign_list]
        )

        new_stats_list = []
        for campaign_stat in campaign_stat_list:
            new_campaign_stat = {}
            for k, v in campaign_stat.items():
                new_campaign_stat.update({"campaign_" + k: v})
            new_stats_list.append(new_campaign_stat)

        for campaign in campaign_list:
            adgroup_id = campaign.get("campaign_nccCampaignId")
            for new_stat in new_stats_list:
                if new_stat.get("campaign_id") == adgroup_id:
                    campaign.update(new_stat)
                    campaign.pop("campaign_nccCampaignId", False)
                else:
                    continue

        # ad_group_info
        adgroup_check_list = [
            "nccAdgroupId",
            "nccCampaignId",
            "mobileChannelId",
            "pcChannelId",
            "name",
            "targets",
            "pcChannelKey",
            "mobileChannelKey",
            "status",
            "expectCost",
            "adgroupAttrJson",
            "adgroupType",
        ]

        # ad_group
        ad_group_list = []
        stat_ids = []
        for campaign in campaign_list:
            adgroup_list = self.get_adgroup(campaign.get("campaign_nccCampaignId"))
            for adgroup in adgroup_list:
                new_adgroup = campaign.copy()

                for k, v in adgroup.items():
                    if k in adgroup_check_list:
                        new_adgroup.update({"adgroup_" + k: v})

                stat_ids.append(new_adgroup.get("adgroup_nccAdgroupId"))
                ad_group_list.append(new_adgroup)

        adgroup_stat_list = self.get_stats(stat_ids)

        new_stats_list = []
        for adgroup in ad_group_list:
            adgroup_id = adgroup.get("adgroup_nccAdgroupId")

            for adgroup_stat in adgroup_stat_list:
                if adgroup_stat.get("id") == adgroup_id:
                    new_stat = adgroup.copy()
                    for k, v in adgroup_stat.items():
                        new_stat.update({"adgroup_" + k: v})
                    new_stats_list.append(new_stat)

        return pd.DataFrame(new_stats_list)

    def get_ads(self, ad_group_id: str):
        """
        reference : http://naver.github.io/searchad-apidoc/#/operations/GET/~2Fncc~2Fads%7B%3FnccAdgroupId%7D
        """

        uri = f"/ncc/ads"
        method = "GET"
        headers = self.__get_header(method, uri)
        params = {"nccAdgroupId": ad_group_id}
        res = requests.get(self.BASE_URL + uri, params=params, headers=headers)
        res.raise_for_status()
        return res.json()

    def get_adgroup(self, campaign_id: str):
        """
        reference : http://naver.github.io/searchad-apidoc/#/operations/GET/~2Fncc~2Fadgroups%7B%3FnccCampaignId,baseSearchId,recordSize,selector%7D
        """

        uri = f"/ncc/adgroups"
        method = "GET"
        headers = self.__get_header(method, uri)
        params = {
            "nccCampaignId": campaign_id,
        }
        res = requests.get(self.BASE_URL + uri, params=params, headers=headers)
        res.raise_for_status()
        time.sleep(0.5)
        return res.json()

    def get_adgroups(self, stat_id: str):
        """
        reference : http://naver.github.io/searchad-apidoc/#/operations/GET/~2Fncc~2Fadgroups%7B%3FnccCampaignId,baseSearchId,recordSize,selector%7D
        """

        uri = "/ncc/adgroups"
        method = "GET"
        headers = self.__get_header(method, uri)
        params = {"nccCampaignId": stat_id}
        res = requests.get(self.BASE_URL + uri, params=params, headers=headers)
        res.raise_for_status()
        return res.json()

    def get_stats(self, stat_ids: list, fields: list = []):
        """
        reference : https://workingwithpython.com/naversearchadapi3/,
                    http://naver.github.io/searchad-apidoc/#/operations/GET/~2Fstats%7B%3Fids,fields,timeRange,datePreset,timeIncrement,breakdown%7D
        """
        uri = "/stats"
        method = "GET"
        headers = self.__get_header(method, uri)

        if not fields:
            fields = self.fields

        params = {
            "ids": stat_ids,
            "fields": json.dumps(fields),
            "timeRange": json.dumps(
                {
                    "since": self.since.strftime(DATE_FORMAT),
                    "until": self.until.strftime(DATE_FORMAT),
                }
            ),
        }
        res = requests.get(self.BASE_URL + uri, params=params, headers=headers)
        time.sleep(0.5)
        res.raise_for_status()
        res = res.json()
        return res.get("data", [])

    def get_campaign_ids(self):
        """
        http://naver.github.io/searchad-apidoc/#/operations/GET/~2Fncc~2Fcampaigns%7B%3Fids%7D
        """
        uri = "/ncc/campaigns"
        method = "GET"
        headers = self.__get_header(method, uri)

        res = requests.get(self.BASE_URL + uri, headers=headers)
        res.raise_for_status()
        return res.json()

    def test(self):
        """
        http://naver.github.io/searchad-apidoc/#/operations/GET/~2Fncc~2Fcampaigns%7B%3Fids%7D
        """
        uri = "/stat-reports"
        method = "GET"
        headers = self.__get_header(method, uri)

        res = requests.get(self.BASE_URL + uri, headers=headers)
        res.raise_for_status()
        return res.json()
