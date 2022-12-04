# -*- coding: utf-8 -*-
import base64
import json
import logging
from io import StringIO

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


class Mixpanel_ingest(object):
    ad = "mixpanel"
    __api_secret: dict = ADS.MIXPANEL["api_secret"]

    def __init__(self, project_name):
        self.project_name = project_name
        api_secret = self.__api_secret.get(project_name, "# credentials")

        self.headers = {
            "Accept": "application/json",
            "Authorization": f"Basic {base64.b64encode((api_secret + ':').encode('ascii')).decode()}",
        }

    def export(self, s=get_yesterday(), u=get_yesterday()):
        self.since, self.until = s, u
        try:
            ### call
            ad = self.ad
            logging.info(f"[{ad}] insight fetch start")
            output = self.get_data()
            logging.info(f"[{ad}] insight fetch success")

            ### save
            return save(f"{ad}_{self.project_name}", output, date=self.until, save_qarquet=True)
        except Exception as e:
            logging.exception(e)
            return None

    def get_data(self):
        url = "https://data.mixpanel.com/api/2.0/export"
        params = {
            "from_date": self.since.strftime(DATE_FORMAT),
            "to_date": self.until.strftime(DATE_FORMAT),
        }
        res = requests.get(url, headers=self.headers, params=params)
        res.raise_for_status()

        s = str(res.content, "utf-8")
        raw = StringIO(s)
        data = pd.DataFrame(raw)
        logging.info(f"requests success")

        data.rename(columns={0: "col"}, inplace=True)

        data["col"] = data["col"].apply(json.loads)
        data = data["col"].apply(pd.Series)
        logging.info(f"apply success")

        data = pd.concat(
            [data["properties"].apply(pd.Series), data.drop(columns=["properties"])],
            axis=1,
        )
        logging.info(f"concat success")
        return data
