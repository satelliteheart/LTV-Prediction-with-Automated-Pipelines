import logging
import pathlib

import numpy as np
import pandas as pd
import pymysql
from credentials import DB
from tools import SAVE_PATH, get_yesterday


class MYSQL:
    def __init__(self):
        self.channel = "db"
        self.connect()
        self.yesterday = get_yesterday().strftime("%Y-%m-%d")

    def connect(self):
        self.conn = pymysql.connect(
            host=DB["host"],
            user=DB["user"],
            password=DB["password"],
            database=DB["database"],
            port=DB["port"],
        )

        self.tables = [
            # credentials
        ]
        self.cur = self.conn.cursor()

    def close(self):
        self.cur.close()
        self.conn.close()

    def export(self, s=get_yesterday(), u=get_yesterday()):
        try:
            self.get_data()
            self.close()
        except Exception as e:
            self.conn.rollback()
            print(e)

        return {"date": f"date={s}", "ad": self.channel}

    def get_data(self):
        tables = self.tables

        for table in tables:
            logging.info(f"{table} - start")
            query = ""  # credentials

            self.cur.execute(query)
            result = self.cur.fetchall()
            step = 100000
            count = result[0][0]
            logging.info(f"{table} - query\n\t: {count}")

            if count > (step * 3):
                logging.info(f"{table} - going to split")
                df = pd.DataFrame()
                start = -1
                while True:
                    cur = self.conn.cursor()
                    query = ""  # credentials
                    logging.info(f"{table} - query\n\t: {query}")
                    cur.execute(query)
                    result = cur.fetchall()

                    if (not result) or (result[-1][0] == start):
                        break

                    field_names = [i[0] for i in cur.description]
                    df = pd.concat([df, pd.DataFrame(result, columns=field_names)], axis=0)
                    start = result[-1][0]
                    # cur.close()
            else:
                logging.info(f"{table} - going to whole")
                cur = self.conn.cursor()
                query = ""  # credentials
                cur.execute(query)

                result = cur.fetchall()
                field_names = [i[0] for i in cur.description]
                df = pd.DataFrame(result, columns=field_names)
                # cur.close()

            path = f"{SAVE_PATH}/{table}"
            pathlib.Path(path).mkdir(parents=True, exist_ok=True)

            df.drop_duplicates(inplace=True)
            df.replace({np.nan: None}, inplace=True)
            df.to_parquet(
                path + f"/date={self.yesterday}" + ".parquet.gz",
                engine="pyarrow",
                compression="gzip",
            )
