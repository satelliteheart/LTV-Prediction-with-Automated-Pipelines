from urllib import parse

from aws import *
from tools import DEFAULT_DATE_FORMAT, get_last_day

GROUPBY = ["media_source", "campaign"]


def get_mixpanel_columns() -> list:
    return [
        # "event_date",
        "event",  # str
        "distinct_id",  # str
        "challenge_id",  # 숫자
        "first",  # bool
        "order_no",  # 숫자
        "order_count",  # 숫자
        "time",  # 숫자
    ]


def get_join_columns() -> list:
    return ["distinct_id", "corp_name", "media_source", "campaign"]


def get_report_columns() -> list:
    return ["key", "value", "channel", "campaign", "adset", "spend", "impressions", "clicks"]


def get_installs(data: DataFrame, groupby):
    print("Get Install Count")
    return (
        data.query("event == '$ae_first_open'")
        .groupby(groupby, dropna=False)
        .agg(unique_installs=("distinct_id", "nunique"), count_installs=("time", "count"))
        .reset_index()
        .fillna({"media_source": "organic", "campaign": "organic"})
    )


def get_signups(data: DataFrame, groupby):
    print("Get Sign Up Count")
    return (
        data.query("event == '# credentails'")
        .groupby(groupby, dropna=False)
        .agg(unique_signups=("distinct_id", "nunique"), count_signups=("time", "count"))
        .reset_index()
        .fillna({"media_source": "organic", "campaign": "organic"})
    )


def get_invites(data: DataFrame, groupby):
    print("Get Invites Count")
    return (
        data.query("event == '# credentails'")
        .groupby(groupby, dropna=False)
        .agg(unique_invites=("distinct_id", "nunique"), count_invites=("time", "count"))
        .reset_index()
        .fillna({"media_source": "organic", "campaign": "organic"})
    )


def get_store_checkout(data: DataFrame, groupby):
    print("Get Store Checkout Count")
    return (
        data.query("event == '# credentails'")
        .groupby(groupby, dropna=False)
        .agg(
            unique_store_checkouts=("distinct_id", "nunique"),
            count_store_checkouts=("time", "count"),
        )
        .reset_index()
        .fillna({"media_source": "organic", "campaign": "organic"})
    )


def get_first_store_checkout(data: DataFrame, groupby):
    try:
        print("Get First Store Checkout Count")
        return (
            data.astype({"order_count": "float"})
            .query("event == '# credentails' & order_count == 1")
            .groupby(groupby, dropna=False)
            .agg(
                unique_first_store_checkouts=("distinct_id", "nunique"),
                count_first_store_checkouts=("time", "count"),
            )
            .reset_index()
            .fillna({"media_source": "organic", "campaign": "organic"})
        )
    except Exception:
        return pd.DataFrame(columns=groupby)


def get_mau(data: DataFrame, groupby):
    print("Get MAU")
    return (
        data.groupby(groupby, dropna=False)
        .agg(mau=("distinct_id", "nunique"))
        .reset_index()
        .fillna({"media_source": "organic", "campaign": "organic"})
    )


def get_dau(data: DataFrame, groupby):
    print("Get DAU")
    return (
        data.groupby(groupby + ["event_date"], dropna=True)
        .agg(dau=("distinct_id", "nunique"))
        .reset_index()
        .groupby(groupby, dropna=False)
        .agg(dau=("dau", "mean"))
        .reset_index()
        .replace({pd.NaT: None})
        .fillna({"media_source": "organic", "campaign": "organic"})
    )


def get_revenue(data: DataFrame, groupby):
    print("Get Revenue")
    return (
        data.astype({"revenue": "float"})
        .query(
            "event.isin([\
                '# credentails',\
                '# credentails',\
                '# credentails'\
            ])"
        )
        .groupby(groupby, dropna=True)
        .agg(revenue=("revenue", "sum"))
        .reset_index()
        .fillna({"media_source": "organic", "campaign": "organic"})
    )


def get_challenge_revenue(data: DataFrame, groupby):
    print("Get Challenge Revenue")
    return (
        data.astype({"revenue": "float"})
        .query("event == '# credentails'")
        .groupby(groupby, dropna=False)
        .agg(challenge_revenue=("revenue", "sum"))
        .reset_index()
        .fillna({"media_source": "organic", "campaign": "organic"})
    )


def get_challenge_brand_revenue(data: DataFrame, groupby):
    print("Get Challenge Brand Revenue")
    return (
        data.astype({"revenue": "float"})
        .query("event == '# credentails'")
        .groupby(groupby, dropna=False)
        .agg(challenge_brand_revenue=("revenue", "sum"))
        .reset_index()
        .fillna({"media_source": "organic", "campaign": "organic"})
    )


def get_store_revenue(data: DataFrame, groupby):
    print("Get Store Revenue")
    return (
        data.astype({"revenue": "float"})
        .query("event == '# credentails'")
        .groupby(groupby, dropna=False)
        .agg(store_revenue=("revenue", "sum"))
        .reset_index()
        .fillna({"media_source": "organic", "campaign": "organic"})
    )


def get_challenge_complete_checkout(data: DataFrame, groupby):
    print("Get Challenge Checkout Count")
    return (
        data.query("event == '# credentails'")
        .groupby(groupby, dropna=False)
        .agg(
            challenge_complete_checkout_count=("time", "count"),
            challenge_complete_checkout_user_count=("distinct_id", "nunique"),
        )
        .reset_index()
        .fillna({"media_source": "organic", "campaign": "organic"})
    )


def get_challenge_complete_checkout_brand(data: DataFrame, groupby):
    print("Get Challenge Checkout Brand Count")
    return (
        data.query("event == '# credentails'")
        .groupby(groupby, dropna=False)
        .agg(
            challenge_complete_checkout_brand_count=("time", "count"),
            challenge_complete_checkout_brand_user_count=("distinct_id", "nunique"),
        )
        .reset_index()
        .fillna({"media_source": "organic", "campaign": "organic"})
    )


def get_first_challenge_complete_checkout(data: DataFrame, groupby):
    print("Get First Challenge Checkout Count")
    return (
        data.query("(event == '# credentails') & (first == True)")
        .groupby(groupby, dropna=False)
        .agg(
            first_challenge_complete_checkout_count=("time", "count"),
            first_challenge_complete_checkout_user_count=("distinct_id", "nunique"),
        )
        .reset_index()
        .fillna({"media_source": "organic", "campaign": "organic"})
    )


def get_first_challenge_complete_checkout_brand(data: DataFrame, groupby):
    print("Get First Challenge Checkout  Brand Count")
    return (
        data.query("(event == '# credentails') & (first == True)")
        .groupby(groupby, dropna=False)
        .agg(
            first_challenge_complete_checkout_brand_count=("time", "count"),
            first_challenge_complete_checkout_brand_user_count=("distinct_id", "nunique"),
        )
        .reset_index()
        .fillna({"media_source": "organic", "campaign": "organic"})
    )


def get_store_complete_checkout(data: DataFrame, groupby):
    print("Get Store Checkout Count")
    return (
        data.query(" event == '# credentails' ")
        .groupby(groupby, dropna=False)
        .agg(
            store_complete_checkout_count=("time", "count"),
            store_complete_checkout_user_count=("distinct_id", "nunique"),
        )
        .reset_index()
        .fillna({"media_source": "organic", "campaign": "organic"})
    )


def get_first_store_complete_checkout(data: DataFrame, groupby):
    print("Get Store Checkout Count")
    return (
        data.query(" (event == '# credentails') & (first==True) ")
        .groupby(groupby, dropna=False)
        .agg(
            first_store_complete_checkout_count=("time", "count"),
            first_store_complete_checkout_user_count=("distinct_id", "nunique"),
        )
        .reset_index()
        .fillna({"media_source": "organic", "campaign": "organic"})
    )


def save_today_raw_data(df: DataFrame, last_1_days):
    ## user 가져오기
    user = pd.concat(
        [
            download_latest("user_challengers", selected_cols=get_join_columns(), _from="raw"),
            download_latest("user_stores", selected_cols=get_join_columns(), _from="raw"),
        ],
        axis=0,
    ).drop_duplicates()

    df = pd.merge(df, user, how="left", on="distinct_id").replace(
        {"": None, "None": None, np.nan: None, "test": None, "테스트": None}
    )

    ### challengers
    BRAND_CHALLENGE_COMPLETE_CHECKOUT = "# credentails"
    CORP_CHALLENGE_COMPLETE_CHECKOUT = "# credentails"
    CHALLENGE_COMPLETE_CHECKOUT = "# credentails"

    event_list = [BRAND_CHALLENGE_COMPLETE_CHECKOUT, CHALLENGE_COMPLETE_CHECKOUT]

    c_df = df[df["event"].isin(event_list)]
    c_df["first"].replace({"None": False, "False": False, "True": True}, inplace=True)

    c_df["# credentails"] = c_df["# credentails"].astype(str)
    c_df["# credentails"] = c_df["# credentails"].astype(str)

    c_db_df = download_latest(
        folder="mysql_calculated/challenge_join",
        selected_cols=[
            # credentails
        ],
        _from="cleansed",
    ).drop_duplicates()

    c_merged = pd.merge(c_df, c_db_df, on=["# credentails"], how="left")

    ## 브랜드 챌린지가 잘 못 찍히고 있는 경우
    c_merged.loc[
        (c_merged["event"] == CHALLENGE_COMPLETE_CHECKOUT) & (c_merged["# credentails"] == "1"),
        "event",
    ] = BRAND_CHALLENGE_COMPLETE_CHECKOUT

    ## 일반 챌린지가 잘 못 찍히고 있는 경우
    c_merged.loc[
        (c_merged["event"] == BRAND_CHALLENGE_COMPLETE_CHECKOUT)
        & (c_merged["# credentails"] == "2"),
        "event",
    ] = CHALLENGE_COMPLETE_CHECKOUT

    ## 기업 챌린지가 잘 못 찍히고 있는 경우
    c_merged.loc[
        (c_merged["# credentails"] == "3"),
        "event",
    ] = CORP_CHALLENGE_COMPLETE_CHECKOUT

    ### stores
    stores_events = ["# credentails"]
    stores_df = df[df["event"].isin(stores_events)]
    stores_df["first"] = stores_df["order_count"].apply(
        lambda x: True if (x == "1.0") or (x == 1) else False
    )
    if "order_no" not in stores_df.columns:
        stores_df["order_no"] = None
    stores_df["order_no"] = stores_df["order_no"].astype(str)

    stores_db_df = download_latest(
        folder="mysql_calculated/pill_join",
        selected_cols=["order_no", "revenue"],
        _from="cleansed",
    )
    stores_db_df["order_no"] = stores_db_df["order_no"].astype(str)

    stores = pd.merge(stores_df, stores_db_df, on="order_no", how="left")
    df = (
        pd.concat(
            [
                c_merged,  # 챌린저스 연산한 것
                stores,  # stores 연산한 것
                df[~df["event"].isin(event_list + stores_events)],  # 그 외 데이터
            ],
            axis=0,
        )
        .replace({np.nan: None})
        .drop_duplicates()
    )
    df["event_date"] = df["time"].apply(
        lambda x: (datetime.fromtimestamp(int(x)) - timedelta(hours=9)).strftime(
            DEFAULT_DATE_FORMAT
        )
    )

    upload(
        data=df[df["event_date"] == last_1_days],
        folder="mixpanel_report",
        title=f"date={last_1_days}",
        _save_raw=True,
    )


def process_mixpanel(offset: int = 1):  # 기본적으로 yesterday를 가져올 수 있도록 설정
    last_1_days = get_last_day(offset + 0).strftime(DEFAULT_DATE_FORMAT)
    last_2_days = get_last_day(offset + 1).strftime(DEFAULT_DATE_FORMAT)
    last_7_days = get_last_day(offset + 6).strftime(DEFAULT_DATE_FORMAT)
    last_30_days = get_last_day(offset + 29).strftime(DEFAULT_DATE_FORMAT)

    ## mixpanel data 가져오기
    # 오늘치 가져오고
    df = download_period(
        folder="mixpanel",
        start=last_2_days,
        end=last_1_days,
        selected_cols=get_mixpanel_columns(),
        _from="cleansed",
    ).drop_duplicates()

    # 오늘치 저장해놓고
    save_today_raw_data(df, last_1_days)

    # 한달치 레포트 생성하러 간다.
    df = download_period(
        "mixpanel_report", start=last_30_days, end=last_1_days, _from="cleansed"
    ).drop_duplicates()

    data = {
        "monthly": df.query(f"event_date <= '{last_1_days}'"),
        "weekly": df.query(f"event_date <= '{last_1_days}' & event_date >= '{last_7_days}'"),
        "daily": df.query(f"event_date == '{last_1_days}'"),
    }

    ad_channel_report = {
        "daily": download_specific(
            folder="channel_report",
            title=f"date={last_1_days}_{last_1_days}",
            selected_cols=get_report_columns(),
            _from="cleansed",
        ),
        "weekly": download_specific(
            folder="channel_report",
            title=f"date={last_7_days}_{last_1_days}",
            selected_cols=get_report_columns(),
            _from="cleansed",
        ),
        "monthly": download_specific(
            folder="channel_report",
            title=f"date={last_30_days}_{last_1_days}",
            selected_cols=get_report_columns(),
            _from="cleansed",
        ),
    }

    for idx in range(len(GROUPBY), 0, -1):
        groupby = GROUPBY[:idx]
        mau = get_mau(data["monthly"], groupby)
        for standard, frame in data.items():
            mau_copy = mau.copy()

            print("=====", standard, GROUPBY[idx - 1], GROUPBY[:idx], idx, "=====")
            buf = (
                mau_copy.merge(get_dau(frame, groupby), how="outer", on=groupby)
                .merge(get_installs(frame, groupby), how="outer", on=groupby)
                .merge(get_signups(frame, groupby), how="outer", on=groupby)
                .merge(get_invites(frame, groupby), how="outer", on=groupby)
                .merge(get_store_checkout(frame, groupby), how="outer", on=groupby)
                .merge(get_first_store_checkout(frame, groupby), how="outer", on=groupby)
                .merge(get_revenue(frame, groupby), how="outer", on=groupby)
                .merge(get_challenge_revenue(frame, groupby), how="outer", on=groupby)
                .merge(get_challenge_brand_revenue(frame, groupby), how="outer", on=groupby)
                .merge(get_store_revenue(frame, groupby), how="outer", on=groupby)
                .merge(get_challenge_complete_checkout(frame, groupby), how="outer", on=groupby)
                .merge(
                    get_challenge_complete_checkout_brand(frame, groupby), how="outer", on=groupby
                )
                .merge(
                    get_first_challenge_complete_checkout(frame, groupby), how="outer", on=groupby
                )
                .merge(
                    get_first_challenge_complete_checkout_brand(frame, groupby),
                    how="outer",
                    on=groupby,
                )
            )

            chunk_report = ad_channel_report[standard].copy()

            if (key := groupby[-1]) == "media_source":
                chunk_report = ad_channel_report[standard].copy()
                chunk_report = (
                    chunk_report[chunk_report["key"] == "channel"]  # 'key' == 'channel'인 것만 가져옴
                    .drop(columns=["channel", "adset", "key", "campaign"])
                    .rename(columns={"value": key})
                )
            elif (key := groupby[-1]) == "campaign":
                chunk_report = ad_channel_report[standard].copy()
                chunk_report = (
                    chunk_report[chunk_report["key"] == "campaign"]  # 'key' == 'campaign'인 것만 가져옴
                    .drop(columns=["adset", "key", "campaign"])
                    .rename(columns={"value": key, "channel": "media_source"})
                )
            else:
                chunk_report = pd.DataFrame(columns=groupby)

            buf = pd.merge(buf, chunk_report, on=groupby, how="outer")
            buf["date"] = last_1_days
            buf.replace({np.nan: None}, inplace=True)

            for unquote_val in ["media_source", "campaign"]:
                if unquote_val in buf.columns:
                    buf[unquote_val] = buf[unquote_val].apply(parse.unquote)

            upload(
                data=buf,
                folder=f"{standard}/{groupby[-1]}",
                title=f"{last_1_days}_000.parquet.gz",
                _to="report",
            )
