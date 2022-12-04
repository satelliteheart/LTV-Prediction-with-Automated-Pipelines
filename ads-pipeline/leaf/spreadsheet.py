# -*- coding: utf-8 -*-
import json
import logging

import gspread
import numpy as np
import pandas as pd
from credentials import ADS
from google.oauth2 import service_account
from gspread.spreadsheet import Spreadsheet, Worksheet
from tools import DEFAULT_DATE_FORMAT, get_last_day, save

logging.basicConfig(
    format="%(asctime)s:%(levelname)s:%(message)s",
    datefmt="%m/%d/%Y %I:%M:%S p",
    level=logging.INFO,
)


class SpreadSheet_ingest(object):
    ad = "spreadsheet"
    json_key = ADS.SPREADSHEET["json_key"]
    url = ADS.SPREADSHEET["url"]
    sheet_name = ADS.SPREADSHEET["sheet_name"]

    need_spreadsheet_list = [
        # credentials
    ]

    def __init__(self):
        self.credentials = service_account.Credentials.from_service_account_file(
            self.json_key,
            scopes=[
                "https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive",
            ],
        )
        self.gc = gspread.authorize(self.credentials)

    def export(self, s=get_last_day(1), u=get_last_day(1)):
        self.since, self.until = s, u
        try:
            ### call
            ad = self.ad
            logging.info(f"[{ad}] insight fetch start")
            self.get_data()
            logging.info(f"[{ad}] insight fetch success")
            return {"date": f"date={self.since.strftime(DEFAULT_DATE_FORMAT)}", "ad": ad}

        except Exception as e:
            logging.exception(e)
            return None

    def __make_new_sheet(self, title) -> Spreadsheet:
        sheet: Spreadsheet = self.gc.create(title)

        with open(self.json_key) as json_file:
            client_email = json.load(json_file).get("client_email")
            print(client_email)

        sheet.share(
            client_email, perm_type="user", role="writer", email_message=False, notify=False
        )

        return sheet

    def __get_headers(self, data: pd.DataFrame, start_row: int, end_row: int) -> list:
        headers = []
        column_name = ""
        raw_data = [list(data.iloc[idx]) for idx in range(start_row, end_row + 1)]
        for idx, column in enumerate(zip(*raw_data)):
            if column[0]:
                column_name = column[0]
            if column[0] or column[1]:
                column_final_name = f"{column_name}-{column[1]}" if column[1] else column_name
            else:
                column_final_name = column[0] or column[1]

            if column_final_name and (column_final_name in headers):
                column_final_name += f"_{idx}"
            headers.append(column_final_name)
        return headers

    def __get_sheet(self, title, sheet_title) -> list:
        try:
            sheet: Spreadsheet = self.gc.open(title)
        except:
            sheet: Spreadsheet = self.__make_new_sheet(title)
        try:
            worksheet = sheet.worksheet(sheet_title)
        except:
            worksheet = sheet.add_worksheet(title=sheet_title, rows=100, cols=20)
        return worksheet

    def __update_to_spreadsheet(
        self, worksheet: Worksheet, data: pd.DataFrame, worksheet_title: str = None
    ):
        # get worksheet
        worksheet = self.__get_sheet(
            title=self.sheet_name,
            sheet_title=f"{worksheet.title}{ '-' + worksheet_title if worksheet_title else ''}",
        )
        data.replace({np.nan: None}, inplace=True)
        # batch update - data가 chunk로 수정됨
        worksheet.batch_clear(["A1:ZZ"])
        worksheet.batch_update(
            [
                {
                    "range": "A1:ZZ",
                    "values": [data.columns.tolist()] + data.values.tolist(),
                },
            ]
        )

        save(ad=f"spreadsheet::{worksheet.title}", output=data, date=self.since, save_qarquet=True)

    def get_data(self):
        # 원본 데이터 가져오기
        sheets = self.gc.open_by_url(self.url)
        for sheet in sheets:
            logging.info(f"{sheet.title} - {sheet.id}")
            try:
                # 스프레드 시트 번호 확인 후 필요한 데이터만 가져온다.
                if sheet.id not in self.need_spreadsheet_list:
                    continue
                worksheet = sheets.worksheet(sheet.title)
                # data cleansing
                output = self.get_worksheet(worksheet)
                if not output:
                    continue

                # 데이터가 있으면 업데이트
                self.__update_to_spreadsheet(worksheet=output[0], data=output[1])
                logging.info(f"save {output[0].title} success")

            except Exception as e:
                logging.error(f"[{self.ad}] {e}")
                continue
        return

    def get_worksheet(self, worksheet: Worksheet) -> list:
        return_set = []
        if worksheet.id == "":  # credentials
            df = self.make_worksheet_1(worksheet)
            return_set = worksheet, df

        elif worksheet.id == "":  # credentials
            df = self.make_worksheet_3(worksheet)
            return_set = worksheet, df

        elif worksheet.id == "":  # credentials
            self.make_worksheet_8(worksheet)

        return return_set

    def make_worksheet_8(self, worksheet: Worksheet):
        raw_data = pd.DataFrame(worksheet.get_all_values())
        data = raw_data.copy()
        raw_headers = self.__get_headers(data, 5, 6)
        headers = raw_headers[:17]
        headers[2] = "Day of Week"
        start = end = 7

        delete_rows = []
        for idx, row in enumerate(data[start:].itertuples()):
            if not row._2:
                end = idx + start
                break
            elif "." not in row._2:
                delete_rows.append(idx + start)

        data = data[(data.index > start - 1) & (data.index < end)]
        data = data.drop(delete_rows)
        data = data.iloc[:, : len(headers)]
        data = pd.DataFrame(data.to_numpy(), columns=headers)

        worksheet_title = ""  # credentials
        self.__update_to_spreadsheet(
            worksheet=worksheet,
            data=data,
            worksheet_title=worksheet_title,
        )
        logging.info(f"save {worksheet_title} success")
        date_format = "%y.%m.%d"
        today = get_last_day().strftime(date_format)
        data = raw_data.copy()

        # get headers
        headers = [raw_headers[1]] + raw_headers[22:]

        start = end = 7
        start_flag = False
        for idx, row in enumerate(data[start:].itertuples()):

            if not start_flag and row._23:
                start = idx + end
                start_flag = True

            if start_flag and not row._23:
                end = idx + end
                break

        data = data.loc[:, [1] + [i for i in range(22, 30)]]
        data = data[(data.index > start - 1) & (data.index < end)]
        for row_idx in delete_rows:
            try:
                data = data.drop([row_idx])
            except:
                continue

        worksheet_title = ""  # credentials
        self.__update_to_spreadsheet(
            worksheet=worksheet,
            data=pd.DataFrame(data.to_numpy(), columns=headers),
            worksheet_title=worksheet_title,
        )
        logging.info(f"save {worksheet_title} success")

    def make_worksheet_3(self, worksheet: Worksheet):
        # get all worksheet data
        data = pd.DataFrame(worksheet.get_all_values())

        # get headers
        headers = self.__get_headers(data, 2, 3)
        headers[0] = "date"
        headers[1] = "Day of Week"

        # get data
        end = start = start_idx = 4

        for idx, row in enumerate(data[start_idx:].itertuples()):
            if not row._4 and row._3 == "₩0":
                end = idx + start_idx + 1
                break

        data = data[(data.index >= start) & (data.index < end)]

        data = pd.DataFrame(data.to_numpy(), columns=headers)
        headers = [header for header in headers if header]

        new_df = pd.DataFrame()

        date_format = "%Y-%m-%d"
        today = get_last_day().strftime(date_format)

        data_period = data[(data["date"] >= "2022-07-01") & (data["date"] <= today)]
        date = data_period.iloc[:, [0]]
        for header in data.columns:
            try:
                [type, channel] = header.split("-")
            except:
                continue

            if type == "광고비":
                hd = "Cost"
            else:
                hd = type

            column_data = pd.concat([date, data_period[header]], axis=1)
            column_data["channel"] = channel
            column_data.rename(columns={header: hd}, inplace=True)
            new_df = pd.concat([new_df, column_data], axis=0)

        return new_df

    def make_worksheet_1(self, worksheet: Worksheet):
        # get all worksheet data
        data = pd.DataFrame(worksheet.get_all_values())

        # get headers
        headers = self.__get_headers(data, 4, 5)
        headers[6] = "date"
        headers[7] = "Day of Week"

        # get data
        end = start = start_idx = 6
        date_format = "%y.%m.%d"
        today = get_last_day().strftime(date_format)

        for idx, row in enumerate(data[start_idx:].itertuples()):
            if row._7 > today:
                end = idx + start_idx
                break

        data = data[(data.index >= start) & (data.index < end)]
        data = pd.DataFrame(data.to_numpy(), columns=headers)
        headers = [header for header in headers if header]

        data.drop(
            columns=[
                # credentials
            ],
            inplace=True,
        )

        data_period = data[(data["date"] >= "22.07.01") & (data["date"] <= today)]
        date = data_period["date"].copy()

        new_df = pd.DataFrame()

        for idx, header in enumerate(data.columns):
            if not header or "date" == header:
                continue
            try:
                [channel, hd] = header.split("-")
            except Exception as e:
                hd = header
                channel = f"total_{header}"
            try:
                facebook_cost = pd.concat([date, data_period[header]], axis=1)
                facebook_cost["channel"] = channel
                facebook_cost.rename(columns={header: hd}, inplace=True)

                new_df = pd.concat([new_df, facebook_cost], axis=0)
            except Exception as e:
                break

        return new_df
