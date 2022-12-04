# -*- coding: utf-8 -*-
import logging

import pandas as pd
from credentials import ADS
from google.ads.googleads import client as g_client
from tools import get_last_day, save

DATE_FORMAT = "%Y-%m-%d"


logging.basicConfig(
    format="%(asctime)s:%(levelname)s:%(message)s",
    datefmt="%m/%d/%Y %I:%M:%S p",
    level=logging.INFO,
)


class Google_ingest(object):
    ad = "google"
    __customer_id = ADS.GOOGLE["customer_id"].replace("-", "")

    def __init__(self):
        self.auth = g_client.GoogleAdsClient.load_from_dict(
            {
                "use_proto_plus": False,
                "developer_token": ADS.GOOGLE["developer_token"],
                "refresh_token": ADS.GOOGLE["refresh_token"],
                "client_id": ADS.GOOGLE["client_id"],
                "client_secret": ADS.GOOGLE["client_secret"],
                "login_customer_id": self.__customer_id,
            }
        )

        self.query = lambda since, until: (
            "SELECT campaign.status, campaign.id, campaign.name, ad_group.id, ad_group.name, ad_group_ad.ad.id, ad_group_ad.ad.name,"
            + "metrics.impressions, metrics.clicks, metrics.average_cost, metrics.interactions,"
            + "metrics.ctr, metrics.average_cpm, metrics.average_cpc, metrics.active_view_cpm,"
            + "metrics.conversions, metrics.all_conversions "
            + f"FROM ad_group_ad WHERE ad_group_ad.status='ENABLED' AND segments.date >= '{since}' AND segments.date <= '{until}'"
        )

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
        except Exception as e:
            logging.exception(e)
            return None

    def get_data(self):
        return self.get_active_ad_report()

    def get_active_ad_report(self):
        client = self.auth.get_service("GoogleAdsService")
        request = self.auth.get_type("SearchGoogleAdsStreamRequest")
        request.customer_id = self.__customer_id
        request.query = self.query(
            self.since.strftime(DATE_FORMAT),
            self.until.strftime(DATE_FORMAT),
        )

        results = client.search_stream(request)
        output = []
        for batch in results:
            for row in batch.results:
                output.append(
                    [
                        str(row.campaign.status),
                        str(row.campaign.id),
                        row.campaign.name,
                        str(row.ad_group.id),
                        row.ad_group.name,
                        str(row.ad_group.id),
                        row.ad_group.name,
                        int(row.metrics.impressions),
                        int(row.metrics.clicks),
                        float(row.metrics.conversions),
                        int(row.metrics.average_cost * row.metrics.interactions / 1000000),
                        float(row.metrics.ctr),
                        float(row.metrics.average_cpm / 1000000),
                        float(row.metrics.average_cpc / 1000000),
                    ]
                )

        if len(output) == 0:
            return pd.DataFrame()

        return pd.DataFrame(
            output,
            columns=[
                "status",
                "campaign_id",
                "campaign",
                "ad_set_id",
                "ad_set",
                "ad_id",
                "ad",
                "impressions",
                "clicks",
                "conversions",
                "cost",
                "ctr",
                "cpm",
                "cpc",
            ],
        )
