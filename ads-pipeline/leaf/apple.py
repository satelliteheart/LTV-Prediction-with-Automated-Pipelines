import json
import logging
from datetime import datetime

import jwt
import requests
from credentials import ADS
from tools import get_yesterday, save

DATE_FORMAT = "%Y-%m-%d"
YESTERDAY = get_yesterday()


logging.basicConfig(
    format="%(asctime)s:%(levelname)s:%(message)s",
    datefmt="%m/%d/%Y %I:%M:%S p",
    level=logging.INFO,
)


class Apple_ingest(object):
    ad = "apple"
    ### auth
    __org_id = ADS.APPLE["team_id"]
    __campaign_id = ADS.APPLE["campaign_id"]

    def export(self):
        try:
            ### call
            ad = self.ad
            logging.info(f"[{ad}] insight fetch start")
            output = self.get_data()
            logging.info(f"[{ad}] insight fetch success")

            return save(ad, output)
        except Exception as e:
            logging.exception(e)
            return None

    def get_data(self):
        return self.generate_creative_report()

    def __generate_client_secret(self):
        headers = {
            "alg": "ES256",
            "kid": ADS.APPLE["keyId"],
        }
        payload = {
            "sub": ADS.APPLE["client_id"],
            "aud": "https://appleid.apple.com",
            "iat": int(datetime.utcnow().timestamp()),
            "exp": int(datetime.utcnow().timestamp()) + 86400 * 180,
            "iss": ADS.APPLE["client_id"],
        }
        client_secret = jwt.encode(
            payload=payload,
            headers=headers,
            algorithm="ES256",
            key=ADS.APPLE["private_key"],
        )
        return client_secret

    def __generate_access_token(self):
        client_id = ADS.APPLE["client_id"]
        client_secret = self.__generate_client_secret()
        url = (
            f"https://appleid.apple.com/auth/oauth2/token?grant_type=client_credentials&"
            + f"client_id={client_id}&client_secret={client_secret}&scope=searchadsorg"
        )
        headers = {
            "Host": "appleid.apple.com",
            "Content-Type": "application/x-www-form-urlencoded",
        }

        res = requests.post(url, headers=headers)
        res = res.json()

        return res["access_token"]

    def get_all_active_campaigns(self):
        access_token = self.__generate_access_token()
        org_id = self.__org_id
        url = "https://api.searchads.apple.com/api/v4/campaigns"
        headers = {"Authorization": f"Bearer {access_token}", "X-AP-Context": f"orgId={org_id}"}
        data = {"limit": 1000}
        res = requests.request("GET", url, headers=headers, params=data)

        res = res.json()["data"]
        ids = []
        for campaign in res:
            if campaign["displayStatus"] == "RUNNING":
                ids.append(campaign["id"])
        with open("testapple.json", "wt", encoding="UTF-8") as outf:
            json.dump(res, outf)

        return ids

    def generate_creative_report(self):
        campaign_id = self.__campaign_id
        org_id = self.__org_id

        url = f"https://api.searchads.apple.com/api/v4/reports/campaigns/{campaign_id}/creativesets"
        access_token = self.__generate_access_token()
        headers = {
            "Authorization": f"Bearer {access_token}",
            "X-AP-Context": f"orgId={org_id}",
        }
        data = {
            "startTime": YESTERDAY,
            "endTime": YESTERDAY,
            "selector": {
                "orderBy": [{"field": "displayStatus", "sortOrder": "ASCENDING"}],
                "conditions": [
                    {"field": "displayStatus", "operator": "EQUALS", "values": ["ELIGIBLE"]}
                ],
                "pagination": {"offset": 0, "limit": 1000},
            },
            "granularity": "DAILY",
            "groupBy": ["countryOrRegion"],
        }
        res = requests.request("POST", url, headers=headers, json=data)
        res.raise_for_status()
        return [res.json().get("data", {}).get("row", [])]

    # getting all ad groups
    def generate_agroup_report(self):
        access_token = self.__generate_access_token()
        org_id = self.__org_id
        campaign_id = self.__campaign_id
        url = f"https://api.searchads.apple.com/api/v4/reports/campaigns/{campaign_id}/adgroups"
        headers = {"Authorization": f"Bearer {access_token}", "X-AP-Context": f"orgId={org_id}"}
        data = {
            "startTime": YESTERDAY,
            "endTime": YESTERDAY,
            "selector": {
                "orderBy": [{"field": "startTime", "sortOrder": "ASCENDING"}],
                "conditions": [
                    {"field": "adGroupStatus", "operator": "EQUALS", "values": ["ENABLED"]}
                ],
                "pagination": {"offset": 0, "limit": 1000},
            },
            "timeZone": "ORTZ",
            "granularity": "DAILY",
            "groupBy": ["countryOrRegion"],
        }
        res = requests.request("POST", url, headers=headers, json=data)
        res = res.json()
        with open("pineapple.json", "wt", encoding="UTF-8") as outf:
            json.dump(res, outf)
        return res
