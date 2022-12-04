import logging

import pandas as pd
from credentials import ADS
from mixpanel_utils import MixpanelUtils
from tools import get_yesterday, save


class MixpanelUser:
    def __init__(self, channel: str):
        self.channel = channel
        self.m = MixpanelUtils(
            ADS.MIXPANEL["api_secret"].get(channel),
            project_id=ADS.MIXPANEL["project_id"].get(channel),
            token=ADS.MIXPANEL["token"],
        )

    def export(self, u=get_yesterday(), s=get_yesterday()):
        res = self.m.query_jql(
            """function main() {
                return People({}).filter(function(user){
                    return (user.properties.media_source !== undefined) & (user.properties.media_source !== null) || 
                    (user.properties.corp_name !== undefined & user.properties.corp_name !== "" & user.properties.corp_name !== " " & user.properties.corp_name !== null);
                })
            }"""
        )

        try:
            people = pd.DataFrame(res)
            people = pd.concat(
                [
                    people.drop(columns=["properties"]),
                    people["properties"].apply(pd.Series),
                ],
                axis=1,
            )
            logging.info(self.channel)
            title = (
                # credentials
            )
            return save(ad=title, output=people, date=u, save_qarquet=True)
        except:
            return {}

    def request_script(self, script: str):
        res = self.m.query_jql(script)
        df = pd.DataFrame(res)

        if df.empty:
            return pd.DataFrame()
        df["properties"] = df["properties"].apply(pd.Series)
        return df
