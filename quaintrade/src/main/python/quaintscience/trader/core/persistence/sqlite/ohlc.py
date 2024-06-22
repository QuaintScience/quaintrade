from typing import Union
import datetime

import pandas as pd

from ..ohlc import OHLCStorageMixin
from .common import SqliteStorage

class SqliteOHLCStorage(SqliteStorage, OHLCStorageMixin):

    def __init__(self,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
    

    def create_tables_impl(self, table_name, conflict_resolution_type: str = "REPLACE"):
        self.connection.execute(f"""CREATE TABLE IF NOT EXISTS {table_name} (date VARCHAR(255) NOT NULL,
                                                                             open REAL NOT NULL,
                                                                             high REAL NOT NULL,
                                                                             low REAL NOT NULL,
                                                                             close REAL NOT NULL,
                                                                             volume INTEGER NOT NULL,
                                                                             oi INTEGER NOT NULL,
                                                                             PRIMARY KEY (date) ON CONFLICT {conflict_resolution_type});""")

    def put(self,
            scrip: str,
            exchange: str,
            df: pd.DataFrame,
            conflict_resolution_type: str = "IGNORE"):
        table_name = self.create_tables(scrip, exchange,
                                        conflict_resolution_type=conflict_resolution_type)
        df.to_sql(table_name, con=self.connection, if_exists="append")
        self.connection.commit()

    def get(self, scrip: str, exchange: str,
            from_date: Union[str, datetime.datetime],
            to_date: Union[str, datetime.datetime],
            conflict_resolution_type: str,
            autofix: bool = True) -> pd.DataFrame:
        cols = ["date", "open", "high", "low",
                "close", "volume", "oi"]
        data = self.get_timestamped_data(scrip, exchange,
                                         table_name_suffixes=[],
                                         from_date=from_date,
                                         to_date=to_date,
                                         cols=cols,
                                         index_col="date",
                                         conflict_resolution_type=conflict_resolution_type)
        if autofix:
            self.logger.warn(f'Fixing {sum(data["low"] == 0) + sum(data["high"] == 0)} rows...')
            data.loc[data["low"] == 0., "low"] = data[data["low"] == 0.].apply(lambda x: min(x["open"], x["close"]), axis=1)
            data.loc[data["high"] == 0., "high"] = data[data["high"] == 0.].apply(lambda x: max(x["open"], x["close"]), axis=1)
            #print("fixed data")
            #print(data)
        return data

    def clear_data(self, scrip: str, exchange: str, conflict_resolution_type: str = "IGNORE"):
        table_name = self.create_tables(scrip, exchange,
                                        conflict_resolution_type=conflict_resolution_type)
        self.connection.execute(f"DROP TABLE IF EXISTS {table_name}")
        table_name = self.create_tables(scrip, exchange,
                                        conflict_resolution_type=conflict_resolution_type)
