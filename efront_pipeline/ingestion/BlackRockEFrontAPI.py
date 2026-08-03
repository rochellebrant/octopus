from pyspark.sql import DataFrame
from requests.cookies import RequestsCookieJar
from typing import Dict
import json
import requests
import base64
from pyspark.sql import functions as f
from databricks.sdk.runtime import *


class BlackRockEFrontAPI:
    # table_name = This is the table created by the oegen team in the efront UI that we want to pull data from

    BASE_URL = "https://oegen.efrontcloud.com"

    def __init__(self, password: str = None, df: DataFrame = None):
        self.scope = "XIO"
        self.username = "OEGENAPI"
        self.secret_key = "blackrock_efront_api_password"
        self.password = dbutils.secrets.get(scope=self.scope, key=self.secret_key)

    def _login(self) -> RequestsCookieJar:
        if not self.password:
            raise ValueError(f"{self.secret_key} environment variable not set")

        url = f"{self.BASE_URL}/api/v1/auth/login"

        cred_string = f"{self.username}:{self.password}"
        cred_bytes = cred_string.encode("ascii")
        cred_base64_bytes = base64.b64encode(cred_bytes)
        cred_base64_string = cred_base64_bytes.decode("ascii")

        headers = {
            "Authorization": f"Basic {cred_base64_string}",
        }
        response = requests.request("GET", url, headers=headers, data={})
        return response.cookies

    def _logout(self, cookies: RequestsCookieJar):
        url = f"{self.BASE_URL}/api/auth/logout"
        requests.request("GET", url, headers={}, data={}, cookies=cookies)

    def get_data(self, endpoint):
        cookies = self._login()
        url = f"{self.BASE_URL}{endpoint}"
        response = requests.request("GET", url, cookies=cookies)
        self._logout(cookies)
        response.raise_for_status()
        return response.json()

    @staticmethod
    def sanitise_column_names(df: DataFrame) -> DataFrame:
        COLUMN_RENAME_MAPPINGS = [
            ("(", "_"),
            (")", "_"),
            (" ", "_"),
            (".", ""),
            ("~", ""),
            (":", ""),
            ("%", "perc"),
        ]
        rename_map = {col: col for col in df.columns}
        for k, v in COLUMN_RENAME_MAPPINGS:
            rename_map = {key: value.replace(k, v) for key, value in rename_map.items()}

        for old_col, new_col in rename_map.items():
            df = df.withColumnRenamed(old_col, new_col)
        return df

    def transform_data(self, df: DataFrame) -> DataFrame:
        df = self.sanitise_column_names(df)
        df = df.dropna(how="all")
        return df.withColumn("datalake_ingestion_timestamp", f.current_timestamp())