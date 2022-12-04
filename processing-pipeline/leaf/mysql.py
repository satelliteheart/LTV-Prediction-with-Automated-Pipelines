import logging

import numpy as np
import pandas as pd
from aws import download, upload
from leaf import calculate, is_instagram_data


def process_db_challenges():

    ## fetch data
    folder = "mysql/# credentails"
    c_db = download(
        folder=folder,
        selected_cols=[
            "id",  # challenge_id
            "goal_id",
            "custom_fields",
        ],
        _from="raw",
    ).rename(columns={"id": "challenge_id"})
    c_db = c_db.astype(str)

    folder = "mysql/# credentails"
    g_db = download(
        folder=folder,
        selected_cols=[
            "id",  # "goal_id"
            "goal_category_id",
        ],
        _from="raw",
    ).rename(columns={"id": "goal_id"})
    g_db = g_db.astype(str)

    folder = "mysql/# credentails"
    g_c_db = download(
        folder=folder,
        selected_cols=[
            "id",  # "goal_category_id"
            "level2",
            "level3",
        ],
        _from="raw",
    ).rename(columns={"id": "goal_category_id"})
    g_c_db = g_c_db.astype(str)

    folder = "mysql/# credentails"
    r_u_c_db = download(
        folder=folder,
        selected_cols=[
            "challenge_id",
            "user_id",
        ],
        _from="raw",
    ).rename(columns={"user_id": "distinct_id"})
    r_u_c_db["distinct_id"] = r_u_c_db["distinct_id"].apply(lambda x: str(int(x)))
    r_u_c_db = r_u_c_db.astype(str)

    folder = "mysql/# credentails"
    c_r_db = download(
        folder=folder,
        selected_cols=[
            "challenge_id",
            "register_count",
            "margin",
        ],
        _from="raw",
    )
    c_r_db["challenge_id"] = c_r_db["challenge_id"].astype(str)

    ## process 1 : goal 연결
    g_i_join = pd.merge(c_db, g_db, how="left", on="goal_id")
    c_g_join = pd.merge(g_i_join, g_c_db, how="left", on="goal_category_id")
    c_g_join.drop(columns=["goal_id", "goal_category_id"], inplace=True)

    ## process 2 : 인스타그램 인증 연결
    c_g_join["need_instagram"] = c_db["custom_fields"].apply(is_instagram_data)
    c_g_join.drop(columns=["custom_fields"], inplace=True)

    ## process 3 : 유저 챌린지 정보와 일반 챌린지 정보 연결 -> ["challenge_id", "user_id", "margin"]
    r_u_c_db = pd.merge(r_u_c_db, c_r_db, how="left", on="challenge_id")

    ## process 4 : 연결 모두 ->  ['challenge_id', 'distinct_id', 'register_count', 'margin', 'level2', 'level3', 'need_instagram']
    c_join = (
        pd.merge(r_u_c_db, c_g_join, how="outer", on="challenge_id")
        .drop(columns=["distinct_id"])
        .drop_duplicates()
    )

    ## 우선 모든 챌린지의 정보 연결하기
    folder = "mysql/# credentails"
    c_r_i_db = download(
        folder=folder,
        selected_cols=[
            "challenge_id",
            "register_count",
            "fee",
        ],
        _from="raw",
    )
    c_r_i_db["challenge_id"] = c_r_i_db["challenge_id"].astype(str)
    c_join = pd.merge(c_join, c_r_i_db, how="left", on="challenge_id")

    ## 제휴 챌린지의 경우
    folder = "mysql/# credentails"
    c_c_db = download(
        folder=folder,
        selected_cols=[
            "id",  # challenge_commerce_id
            "challenge_id",
        ],
        _from="raw",
    ).rename(columns={"id": "challenge_commerce_id"})
    c_c_db = c_c_db.astype(str)

    folder = "mysql/# credentails"
    r_c_c_p_db = download(
        folder=folder,
        selected_cols=[
            "challenge_commerce_id",
            "product_id",
        ],
        _from="raw",
    )
    r_c_c_p_db = r_c_c_p_db.astype(str)

    folder = "mysql/# credentails"
    p_db = download(
        folder=folder,
        selected_cols=[
            "id",  # product_id
            "name",
            "price",
            "fee",
        ],
        _from="raw",
    ).rename(columns={"id": "product_id", "fee": "product_fee"})
    p_db["product_id"] = p_db["product_id"].astype(str)

    ### process
    c_c_i_join = pd.merge(
        r_c_c_p_db,
        c_c_db,
        how="left",
        on="challenge_commerce_id",
    )
    c_b_join = pd.merge(
        c_c_i_join,
        p_db,
        how="left",
        on="product_id",
    )
    c_db = pd.merge(
        c_join,
        c_b_join,
        how="outer",
        on="challenge_id",
    ).drop_duplicates()

    # 4. calculate
    logging.info("check brand challenges & revenue_from_advertise part")
    c_db.replace({np.nan: None}, inplace=True)
    c_db[
        [
            # credentails
        ]
    ] = c_db[
        [
            # credentails
        ]
    ].apply(
        lambda x: calculate(x), axis=1
    )

    ## 저장하기
    logging.info("save")
    upload(data=c_db, folder="mysql_calculated/challenge_join", _to="cleansed")


def process_db_stores():
    # stores
    folder = "mysql/# credentails"
    p_o_db = download(
        folder=folder,
        selected_cols=[
            "id",  # pill_order_id
            "order_no",
        ],
        _from="raw",
    )
    p_o_db.rename(columns={"id": "pill_order_id"}, inplace=True)

    folder = "mysql/# credentails"
    p_o_c_i_db = download(
        folder=folder,
        selected_cols=[
            "id",  # pill_order_id
            "pill_option_id",
        ],
        _from="raw",
    )
    p_o_c_i_db.rename(columns={"id": "pill_order_id"}, inplace=True)

    folder = "mysql/# credentails"
    p_o_db = download(
        folder=folder,
        selected_cols=[
            "id",
            "discounted_price",
            "fee_ratio",
            "reward_amount",
        ],
        _from="raw",
    )
    p_o_db.rename(columns={"id": "pill_option_id"}, inplace=True)

    ## join process
    p_o_join = pd.merge(p_o_db, p_o_c_i_db, how="inner", on="pill_order_id")
    p_join = pd.merge(p_o_join, p_o_db, how="inner", on="pill_option_id")

    # cleansing
    p_join.drop(columns=["pill_order_id", "pill_option_id"], inplace=True)

    p_join.drop_duplicates(inplace=True)
    p_join["revenue"] = p_join[["discounted_price", "fee_ratio", "reward_amount"]].apply(
        lambda x: (int(x[0]) * float(x[1])) + float(x[2]), axis=1
    )
    upload(data=p_join, folder="mysql_calculated/pill_join", _to="cleansed")
