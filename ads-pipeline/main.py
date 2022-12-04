# -*- coding: utf-8 -*-
import logging

from aws import Upload
from leaf import Adison, Appsflyer, Facebook, Google, Mixpanel, MixpanelUser, Naver, SpreadSheet
from mysql import MYSQL
from tools import TODAY, cleanup_data


def facebook_ingestion():
    date = Facebook().export()
    Upload().upload(date)


def naver_ingestion():
    date = Naver().export()
    Upload().upload(date)


def adison_ingestion():
    date = Adison().export()
    Upload().upload(date)


def google_ingestion():
    date = Google().export()
    Upload().upload(date)


def spreadsheet_ingestion():
    date = SpreadSheet().export()
    Upload().upload(date)


def appsflyer_ingestion():
    Appsflyer().export()


def db_ingestion():
    date = MYSQL().export()
    Upload().upload(date)


def mixpanel_ingestion():
    date = Mixpanel("# credentials").export()
    Upload().upload(date)
    date = Mixpanel("# credentials").export()
    Upload().upload(date)


def mixpanel_user():
    date = MixpanelUser("# credentials").export()
    Upload().upload(date)
    date = MixpanelUser("# credentials").export()
    Upload().upload(date)


def mixpanel():
    mixpanel_ingestion()
    mixpanel_user()


if __name__ == "__main__":
    try:
        print(TODAY)
        cleanup_data()

        appsflyer_ingestion()
        naver_ingestion()
        adison_ingestion()
        google_ingestion()

        facebook_ingestion()
        spreadsheet_ingestion()
        mixpanel()
        db_ingestion()

    except Exception as e:
        logging.exception(e.__str__())
