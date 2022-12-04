import pandas as pd
from aws import *
from credentials import AWS
from tools import DEFAULT_DATE_FORMAT, get_last_day


def get_appsflyer_columns():
    return [
        "Install Time",
        "Event Time",
        "Event Name",
        "Event Value",
        "Media Source",
        "Channel",
        "Keywords",
        "Campaign",
        "Campaign ID",
        "Adset",
        "Adset ID",
        "Ad",
        "Ad ID",
        "Ad Type",
        "AppsFlyer ID",
        "Customer User ID",
        "App ID",
        "appsflyer_type",
    ]


def get_n_days_funnel(df: DataFrame, title: str, last_1_days: str = None):
    #
    total_visit = df["appsflyer_id"].nunique()
    total_visit_uid = df["user_id"].nunique()
    total_install = df[df["event"] == "install"]["appsflyer_id"].nunique()
    total_install_uid = df[df["event"] == "install"]["user_id"].nunique()

    install_ids = df[df["event"] == "install"]["appsflyer_id"].unique()
    total_install_signup = df[
        df["appsflyer_id"].isin(install_ids) & (df["event"] == "# credentails")  # 회원가입
    ]["appsflyer_id"].nunique()
    install_uids = df[df["event"] == "install"]["user_id"].unique()
    total_install_signup_uid = df[
        df["user_id"].isin(install_uids) & (df["event"] == "# credentails")
    ]["user_id"].nunique()

    signup_ids = df[df["event"] == "# credentails"]["appsflyer_id"].unique()
    total_signup_view = df[
        df["appsflyer_id"].isin(signup_ids) & (df["event"] == "af_content_view")  # 상세 보기
    ]["appsflyer_id"].nunique()
    signup_uids = df[df["event"] == "# credentails"]["user_id"].unique()
    total_signup_view_uid = df[
        df["user_id"].isin(signup_uids) & (df["event"] == "af_content_view")
    ]["user_id"].nunique()

    view_ids = df[df["event"] == "af_content_view"]["appsflyer_id"].unique()
    total_view_purchase = df[
        df["appsflyer_id"].isin(view_ids) & (df["event"] == "apply_complete_payment_server")  # 구매
    ]["appsflyer_id"].nunique()
    total_view_store_purchase = df[
        df["appsflyer_id"].isin(view_ids) & (df["event"] == "store_complete_checkout")  # 구매 - 스토어
    ]["appsflyer_id"].nunique()

    view_uids = df[df["event"] == "af_content_view"]["user_id"].unique()
    total_view_purchase_uid = df[
        df["user_id"].isin(view_uids) & (df["event"] == "apply_complete_payment_server")
    ]["user_id"].nunique()
    total_view_store_purchase_uid = df[
        df["user_id"].isin(view_uids) & (df["event"] == "store_complete_checkout")
    ]["user_id"].nunique()

    upload(
        DataFrame(
            {
                "date": [last_1_days],
                "total_visit": [total_visit],
                "total_visit_uid": [total_visit_uid],
                "total_install": [total_install],
                "total_install_uid": [total_install_uid],
                "total_install_signup": [total_install_signup],
                "total_install_signup_uid": [total_install_signup_uid],
                "total_signup_view": [total_signup_view],
                "total_signup_view_uid": [total_signup_view_uid],
                "total_view_purchase": [total_view_purchase],
                "total_view_purchase_uid": [total_view_purchase_uid],
                "total_view_store_purchase": [total_view_store_purchase],
                "total_view_store_purchase_uid": [total_view_store_purchase_uid],
                "install_ids_len": [len(install_ids)],
                "install_uids_len": [len(install_uids)],
                "signup_ids_len": [len(signup_ids)],
                "signup_uids_len": [len(signup_uids)],
                "view_ids_len": [len(view_ids)],
                "view_uids_len": [len(view_uids)],
                "install_signup": [
                    (total_install_signup / len(install_ids) * 100) if len(install_ids) > 0 else 0
                ],  # 설치 -> 회원가입
                "install_signup_uid": [
                    (total_install_signup_uid / len(install_uids) * 100)
                    if len(install_uids) > 0
                    else 0
                ],  # 설치 -> 회원가입
                "signup_view": [
                    (total_signup_view / len(signup_ids) * 100) if len(signup_ids) > 0 else 0
                ],  # 회원가입 -> 상세보기
                "signup_view_uid": [
                    (total_signup_view_uid / len(signup_uids) * 100) if len(signup_uids) > 0 else 0
                ],  # 회원가입 -> 상세보기
                "view_purchase": [
                    (total_view_purchase / len(view_ids) * 100) if len(view_ids) > 0 else 0
                ],  # 상세보기 -> 구매
                "view_purchase_uid": [
                    (total_view_purchase_uid / len(view_uids) * 100) if len(view_uids) > 0 else 0
                ],  # 상세보기 -> 구매
                "view_store_purchase": [
                    (total_view_store_purchase / len(view_ids) * 100) if len(view_ids) > 0 else 0
                ],  # 보기->스토어
                "view_store_purchase_uid": [
                    (total_view_store_purchase_uid / len(view_uids) * 100)
                    if len(view_uids) > 0
                    else 0
                ],  # 보기->스토어
            }
        ),
        f"appsflyer/{title}",
        f"date={last_1_days}",
    )


def save_today_raw_data(df: DataFrame, last_1_days):
    df = df[get_appsflyer_columns()]
    df.rename(
        columns={
            "Event Name": "event",
            "Event Value": "event_value",
            "Install Time": "install_time",
            "Event Time": "time",
            "Customer User ID": "user_id",
            "AppsFlyer ID": "appsflyer_id",
            "App ID": "app_id",
        },
        inplace=True,
    )

    df["event_date"] = df["time"].apply(
        lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S").strftime(DEFAULT_DATE_FORMAT)
    )
    upload(df, "appsflyer/data", f"date={last_1_days}")


def process_funnel(offset: int = 1):
    """
    11/24 작업 완료
    """
    last_1_days = get_last_day(offset + 0).strftime(DEFAULT_DATE_FORMAT)
    last_7_days = get_last_day(offset + 6).strftime(DEFAULT_DATE_FORMAT)
    last_30_days = get_last_day(offset + 29).strftime(DEFAULT_DATE_FORMAT)

    logging.info(f"last_1_days: {last_1_days}")
    download_appsflyer(date=last_1_days, folder="challengers/appsflyer", _from="maymay")
    df = DataFrame()
    for keyword in ["inapp", "install", "org_inapp", "org_install"]:
        df1 = pd.concat(
            [
                pd.read_csv(
                    f"appsflyer/{AWS.APPSFLYER_IOS}_{last_1_days}_{last_1_days}_{keyword}.csv.gz",
                    low_memory=False,
                ),
                pd.read_csv(
                    f"appsflyer/{AWS.APPSFLYER_AOS}_{last_1_days}_{last_1_days}_{keyword}.csv.gz",
                    low_memory=False,
                ),
            ],
            axis=0,
        )
        df1["appsflyer_type"] = keyword
        df = pd.concat([df, df1], axis=0)
    save_today_raw_data(df=df, last_1_days=last_1_days)

    # 계산
    df = download_period("appsflyer/data", start=last_30_days, end=last_1_days, _from="cleansed")
    df.drop_duplicates(inplace=True)

    time = sorted(df["event_date"].unique().tolist())[0]
    if time == last_30_days:
        get_n_days_funnel(
            df=df,
            title="monthly",
            last_1_days=last_1_days,
        )

    if time <= last_7_days:
        get_n_days_funnel(
            df=df.query(f"event_date >= '{last_7_days}'"),
            title="weekly",
            last_1_days=last_1_days,
        )

    if time <= last_1_days:
        get_n_days_funnel(
            df=df.query(f"event_date >= '{last_1_days}'"),
            title="daily",
            last_1_days=last_1_days,
        )
