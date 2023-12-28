from abc import abstractmethod, ABC
from typing import Union
import sqlite3
import datetime

import pandas as pd

from .util import get_datetime


class OHLCStorage(ABC):

    def __init__(self,
                 path: str):
        self.path = path
        self.connect()
    
    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def put(self, scrip: str, exchange: str, df: pd.DataFrame):
        pass

    @abstractmethod
    def get(self, scrip: str, exchange: str,
            fromdate: Union[str, datetime.datetime],
            todate: Union[str, datetime.datetime]) -> pd.DataFrame:
        pass



class SqliteOHLCStorage(OHLCStorage):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    

    def connect(self):
        self.connection = sqlite3.connect(self.path)
    
    def create_ohlc_table(self, table_name):
        self.connection.execute("""CREATE TABLE IF NOT EXISTS ADANIENT (date VARCHART(255) NOT NULL,
                                                                        open REAL NOT NULL,
                                                                        high REAL NOT NULL,
                                                                        low REAL NOT NULL,
                                                                        close REAL NOT NULL,
                                                                        volume INTEGER NOT NULL,
                                                                        oi INTEGER NOT NULL,
                                                                        PRIMARY KEY (date) ON CONFLICT IGNORE);""")

    def __table_name(self, scrip: str, exchange: str):
        return f"{scrip}__{exchange}"

    def put(self, scrip: str, exchange: str, df: pd.DataFrame):
        df.to_sql(self.__table_name(scrip, exchange), con=self.connection, if_exists="append")

    def get(self, scrip: str, exchange: str,
            fromdate: Union[str, datetime.datetime],
            todate: Union[str, datetime.datetime]) -> pd.DataFrame:

        fromdate = get_datetime(fromdate).strftime("%Y-%m-%d %H:%M:%S")
        todate = get_datetime(todate).strftime("%Y-%m-%d %H:%M:%S")

        data = self.connection.execute(f"SELECT date, open, high, low, close, volume, oi FROM {self.__table_name(scrip, exchange)} WHERE datetime(date) BETWEEN '{fromdate}' AND '{todate}';").fetchall()
        data = pd.DataFrame(data, columns=["date", "open", "high", "low", "close", "volume", "oi"])
        data.index = data["date"]
        data.drop(["date"], axis=1, inplace=True)
        return data
