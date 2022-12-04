import pathlib

from aws import DataReader
from tools import (
    COLUMNS_MERGE_MAP,
    REPLACE_VALUE,
    check_if_custom_id_type,
    check_if_uuid_type,
    is_true,
    json_decoding,
    string_to_float,
    string_to_int,
)


class DataCleanser(DataReader):
    def __init__(self, offset: int = 1):
        super().__init__(offset)

    def get_data_type(self, date=None):
        if not date:
            date = self.date

        data = self.load_type_by_date(date)
        self.data["# credentials"] = self.data["# credentials"].apply(json_decoding)
        self.data["# credentials"] = self.data["# credentials"].apply(
            lambda x: [x] if not isinstance(x, list) and x is not None and x > 0 else x
        )
        self.data = self.data.explode("# credentials", ignore_index=True)

        self.data["# credentials"] = self.data["# credentials"].astype(str).replace({"None": None})
        self.data.loc[self.data["# credentials"].isin(data["brand"]), "event"] = (
            "brand_" + self.data["event"]
        )
        self.data.loc[self.data["# credentials"].isin(data["corp"]), "event"] = (
            "corp_" + self.data["event"]
        )

    def type_casting(self):
        for col in ["# credentials"]:
            if col in self.data.columns:
                self.data[col] = self.data[col].apply(
                    lambda x: string_to_int(x) if "_id" not in col else str(string_to_int(x))
                )
                self.data[col].replace(REPLACE_VALUE, inplace=True)

        for col in ["# credentials"]:
            if col in self.data.columns:
                self.data[col] = self.data[col].apply(is_true)

    def columns_merge(self):
        self.data.rename(
            columns={
                # credentials
            },
            inplace=True,
        )
        for col, tar in COLUMNS_MERGE_MAP:
            if col in self.data.columns:
                self.data.loc[self.data[col].notnull(), tar] = self.data[col].copy()
                self.data.drop(col, axis=1, inplace=True)

        if "# credentials" in self.data.columns:
            self.data["# credentials"] = (
                self.data["# credentials"].fillna("") + "/" + self.data["# credentials"].fillna("")
            )
            self.data["# credentials"].replace({"/": None}, inplace=True)
            self.data.drop("# credentials", axis=1, inplace=True)

        if "# credentials" in self.data.columns:
            self.data.loc[self.data["# credentials"] == "# credentials", "event"] = (
                "brand_" + self.data["event"]
            )
            self.data.drop("# credentials", axis=1, inplace=True)

        if "# credentials" in self.data.columns:
            self.data.loc[
                (self.data["event"] == "# credentials")
                & (
                    self.data["total_deposit_amount"].isna()
                    | (self.data["total_deposit_amount"].apply(string_to_float) == 0)
                ),
                "event",
            ] = "zero_# credentials"
            self.data.drop("total_deposit_amount", axis=1, inplace=True)

        if "# credentials" in self.data.columns:
            self.data.loc[
                (self.data["event"] == "# credentials_on_cart")
                & (
                    self.data["# credentials"].isna()
                    | (self.data["# credentials"].apply(string_to_float) == 0)
                ),
                "event",
            ] = "zero_# credentials"
            self.data.drop("# credentials", axis=1, inplace=True)

    def id_mapping(self):
        for col in ["# credentials"]:
            if col in self.data.columns:
                self.data.loc[
                    (
                        self.data["# credentials"].apply(check_if_uuid_type)
                        & self.data[col].apply(check_if_custom_id_type)
                    ),
                    "# credentials",
                ] = self.data[col]
                self.data.drop(col, axis=1, inplace=True)

        user = self.data[["# credentials"]]
        user = user[user["# credentials"].apply(check_if_custom_id_type)]
        user = (
            user.sort_values("# credentials", ascending=False)
            .groupby("# credentials")
            .head(1)
            .set_index("# credentials")["# credentials"]
            .to_dict()
        )

        self.data["# credentials"] = self.data.apply(
            lambda x: user[x["# credentials"]]
            if x["# credentials"] in user.keys()
            else x["# credentials"],
            axis=1,
        )

    def drop_useless(self):
        self.data.replace(REPLACE_VALUE, inplace=True)

        self.data.sort_values("# credentials", inplace=True)
        self.data = self.data[
            self.data["# credentials"].notnull() & ~self.data["event"].isin(["# credentials"])
        ]
        self.data = (
            self.data.groupby(sorted(set(self.data.columns) - set(["# credentials"])), dropna=False)
            .agg({"# credentials": "unique"})
            .reset_index()
        )

        self.data.rename(columns={"# credentials"}, inplace=True)
        for col in self.data.columns:
            self.data[col] = self.data[col].astype(str)

        self.data.replace(REPLACE_VALUE, inplace=True)

    def save_and_upload(self):
        pathlib.Path("# credentials").mkdir(parents=True, exist_ok=True)

        self.data.to_parquet(f"/tmp/date={self.date}.parquet.gz", index=False, compression="gzip")
        self.s3.upload_file(
            f"/tmp/date={self.date}.parquet.gz",
            "# credentials",
            f"mixpanel/date={self.date}.parquet.gz",
        )
