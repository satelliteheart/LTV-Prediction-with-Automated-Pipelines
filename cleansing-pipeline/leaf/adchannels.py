import pandas as pd
from aws import S3
from pandas import DataFrame
from tools import DEFAULT_DATE_FORMAT, cleanse_number_types, get_last_day


def adreport_groupby(org_df: DataFrame):
    df = pd.DataFrame()
    check_list = ["channel", "campaign", "adset", "ad"]

    for idx, key in enumerate(check_list):
        new_df = org_df.copy()
        new_df = new_df.groupby(check_list[: idx + 1], group_keys=True).sum(numeric_only=True)
        new_df.reset_index(inplace=True)

        if "index" in new_df.columns:
            new_df.drop(columns=["index"], inplace=True)
        new_df["key"] = key
        new_df.rename(columns={key: "value"}, inplace=True)
        df = pd.concat([df, new_df], axis=0)

    return df


def naver(df: DataFrame):
    # rename
    rename_column = {
        "campaign_name": "campaign",
        "adgroup_name": "adset",
        "ad_name": "ad",
        ###
        "campaign_status": "status",
        "Date": "date",
        ###
        "adgroup_id": "adset_id",
        # "adgroup_nccAdgroupId": "adset_id",
        "adgroup_nccCampaignId": "campaign_id",
        "adgroup_status": "status",
        "adgroup_ctr": "ctr",
        "adgroup_clkCnt": "clicks",
        "adgroup_ror": "ror",
        "adgroup_viewCnt": "views",
        "adgroup_cpc": "cpc",
        "adgroup_ccnt": "conversions",
        "adgroup_impCnt": "impressions",
        "adgroup_salesAmt": "spend",
    }
    df.rename(columns=rename_column, inplace=True)

    drop_column = [
        "campaign_campaignTp",
        "campaign_totalChargeCost",
        "status",
        "campaign_expectCost",
        "campaign_ctr",
        "campaign_convAmt",
        "campaign_clkCnt",
        "campaign_crto",
        "campaign_ror",
        "campaign_viewCnt",
        "campaign_cpc",
        "campaign_ccnt",
        "campaign_id",
        "campaign_cpConv",
        "campaign_impCnt",
        "campaign_salesAmt",
        "adgroup_mobileChannelId",
        "adgroup_pcChannelId",
        "adgroup_pcChannelKey",
        "adgroup_mobileChannelKey",
        "adgroup_nccAdgroupId",
        "status",
        "adgroup_expectCost",
        "adgroup_adgroupAttrJson",
        "adgroup_adgroupType",
        "adgroup_convAmt",
        "adgroup_crto",
        "adgroup_cpConv",
    ]
    df.drop(columns=drop_column, inplace=True)

    # type change
    change_string_to_float = [
        "ctr",
        "clicks",
        "ror",
        "views",
        "cpc",
        "conversions",
        "impressions",
        "spend",
    ]
    for col in change_string_to_float:
        df[col] = df[col].apply(cleanse_number_types)

    df = df[df["spend"] > 0]

    df["channel"] = "naver"
    return df


def facebook(df: DataFrame):
    # rename
    rename_column = {
        "Date": "date",
        "campaign_name": "campaign",
        "adset_name": "adset",
        "ad_name": "ad",
    }
    df.rename(columns=rename_column, inplace=True)

    # drop columns

    # type change
    change_string_to_float = [
        "spend",
        "impressions",
        "clicks",
        "reach",
        "cpm",
        "ctr",
        "cpp",
    ]
    for col in change_string_to_float:
        df[col] = df[col].apply(cleanse_number_types)

    df["channel"] = "facebook"
    return df


def google(df: DataFrame):

    df = df[df["cost"] > "0"]

    # rename
    rename_column = {
        "Date": "date",
        "ad_set_id": "adset_id",
        "ad_set": "adset",
        "cost": "spend",
    }
    df.rename(columns=rename_column, inplace=True)

    # drop columns

    # type change
    change_string_to_float = [
        "cpm",
        "cpc",
        "ctr",
        "impressions",
        "clicks",
        "conversions",
        "spend",
        "status",
    ]
    for col in change_string_to_float:
        df[col] = df[col].apply(cleanse_number_types)

    df["channel"] = "google"
    return df


def adison(df: DataFrame):
    # rename
    rename_column = {
        "Date": "date",
        "채널": "channel",
        "플랫폼": "platform",
        "캠페인": "campaign",
        "오퍼월 리스트 노출": "impressions",
        "클릭": "clicks",
        "참여": "participations",
        "유니크 참여": "unique_participations",
        "완료": "completes",
        "CTR": "ctr",
        "CVR": "cvr",
        "광고비": "spend",
    }
    df.rename(columns=rename_column, inplace=True)

    # drop columns
    drop_column = ["Unnamed: 0", "리포팅 기간", "광고단가", "channel"]
    df.drop(columns=drop_column, inplace=True)

    # type change
    change_string_to_float = [
        "impressions",
        "clicks",
        "participations",
        "unique_participations",
        "completes",
        "ctr",
        "cvr",
        "spend",
    ]
    for col in change_string_to_float:
        df[col] = df[col].apply(cleanse_number_types)

    df["channel"] = "adison"
    return df


def spreadsheet(s3bucket: S3):
    offset = 1
    data = DataFrame()
    # get latest data
    while True:
        last_day = get_last_day(offset).strftime(DEFAULT_DATE_FORMAT)
        offset += 1
        data = s3bucket.get_objects_by_regex(f"spreadsheet/\(auto\)_광고비/date={last_day}(.*)")
        if not data.empty:
            break
    return data


def pincrux(df: DataFrame, s3bucket: S3):
    # drop columns
    drop_column = ["Unnamed: 0", "Date", "conversion_rate", "channel", "campaign"]
    df.drop(columns=drop_column, inplace=True)

    # rename
    rename_column = {
        "complete": "completes",
    }
    df.rename(columns=rename_column, inplace=True)

    # type change
    change_string_to_float = ["clicks", "completes"]
    for col in change_string_to_float:
        df[col] = df[col].apply(cleanse_number_types)

    df = df.groupby("date", group_keys=True).sum()
    df.reset_index(inplace=True)

    df_spdsht = spreadsheet(s3bucket=s3bucket)
    for idx, row in df.iterrows():
        date = row.iloc[0]
        spend = (
            df_spdsht[
                (df_spdsht["channel"] == "Pincrux")
                & (df_spdsht["date"] == date)
                & (df_spdsht["Cost"] != "None")
            ]["Cost"].tolist()
        ) or [None]
        df.loc[idx, "spend"] = spend[0]

    change_string_to_float = ["spend"]
    for col in change_string_to_float:
        df[col] = df[col].apply(cleanse_number_types)

    df["channel"] = "pincrux"
    return df
