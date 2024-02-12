from abc import abstractmethod, ABC
from typing import Union, Optional
<<<<<<< HEAD
from threading import Lock
=======
>>>>>>> 1e314e13b6fa1d64fdc5ea31562aa7266bece468
import sqlite3
import datetime

import pandas as pd

from ..common import Storage
from ...util import sanitize, get_datetime


class SqliteStorage(Storage):

    def __init__(self, *args, **kwargs):
        self.cache = {}
<<<<<<< HEAD
        self.write_lock = Lock()
=======
>>>>>>> 1e314e13b6fa1d64fdc5ea31562aa7266bece468
        super().__init__(*args, **kwargs)

    def init_cache_for(self, *args,
                       conflict_resolution_type: str = "REPLACE"):
        key = self.get_table_name(*args)
<<<<<<< HEAD
        with self.write_lock:
            self.create_tables(*args,
                            conflict_resolution_type=conflict_resolution_type)
=======
        self.create_tables(*args,
                           conflict_resolution_type=conflict_resolution_type)
>>>>>>> 1e314e13b6fa1d64fdc5ea31562aa7266bece468
        if key not in self.cache:
            self.cache[key] = {}
            for k in self.table_names:
                self.cache[key][k] = []
        return key

    def commit(self):
        for key, all_data in self.cache.items():
            for table_suffix, data in all_data.items():
                if len(data) > 0:
<<<<<<< HEAD
                    self.logger.debug(f"Writing cache for {key} / {table_suffix} with {len(data)} to {self.path}")
                    df = pd.DataFrame(data)
                    with self.write_lock:
                        df.to_sql(f"{key}__{table_suffix}",
                                con=self.connection,
                                if_exists="append",
                                index=False)
        self.cache = {}

    def connect(self):
        self.logger.debug(f"Connecting to {self.path}")
        self.connection = sqlite3.connect(self.path, check_same_thread=False)
=======
                    self.logger.info(f"Writing cache for {key} / {table_suffix} with {len(data)} to {self.path}")
                    df = pd.DataFrame(data)
                    df.to_sql(f"{key}__{table_suffix}",
                            con=self.connection,
                            if_exists="append",
                            index=False)
        self.cache = {}

    def connect(self):
        self.logger.info(f"Connecting to {self.path}")
        self.connection = sqlite3.connect(self.path)
>>>>>>> 1e314e13b6fa1d64fdc5ea31562aa7266bece468

    def get_table_name(self, *args):
        return "__".join([sanitize(str(arg)) for arg in args])

    def create_tables(self, *args,
                      conflict_resolution_type: str = "IGNORE"):
        table_name = self.get_table_name(*args)
        self.create_tables_impl(table_name,
                                conflict_resolution_type)
        return table_name

    def __date_parse(self, from_date, to_date):
        if from_date is None:
            from_date = datetime.datetime.now() - datetime.timedelta(days=100000)
        if to_date is None:
            to_date = datetime.datetime.now()
        from_date = get_datetime(from_date).strftime("%Y-%m-%d %H:%M:%S")
        to_date = get_datetime(to_date).strftime("%Y-%m-%d %H:%M:%S")
        return from_date, to_date

    @abstractmethod
    def create_tables_impl(self, table_name, conflict_resolution_type: str = "IGNORE"):
        pass

    def get_timestamped_data(self, 
                             *args,
                             table_name_suffixes: Optional[list] = None,
                             from_date: Optional[Union[str, datetime.datetime]] = None,
                             to_date: Optional[Union[str, datetime.datetime]] = None,
                             data_name: str = "data",
                             cols: list = None,
                             index_col: str = "date",
                             col_filters: Optional[dict] = None,
                             skip_time_stamps: bool = False,
                             conflict_resolution_type: str = "IGNORE"):
        self.create_tables(*args,
                           conflict_resolution_type=conflict_resolution_type)
        if table_name_suffixes is None:
            table_name_suffixes = []
<<<<<<< HEAD
        table_name = self.create_tables(*args)
        if len(table_name_suffixes) > 0:
            table_name = f"{table_name}__{'__'.join(table_name_suffixes)}"
        if cols is None or len(cols) == 0:
            self.logger.info(f"Inferring column names for {table_name}")
            cursor = self.connection.execute(f"SELECT * from {table_name} LIMIT 1;")
            cols = list(map(lambda x: x[0], cursor.description))
            self.logger.info(f"Inferred col names for {table_name}: {', '.join(cols)}")

        from_date, to_date = self.__date_parse(from_date, to_date)
        self.logger.debug(f"Reading {data_name} from {from_date} to {to_date} from {table_name}...")

        if "date" not in cols and not skip_time_stamps:
=======
        if cols is None or len(cols) == 0:
            raise ValueError("Cols not specified to fetch data")

        table_name = self.create_tables(*args)

        from_date, to_date = self.__date_parse(from_date, to_date)
        if len(table_name_suffixes) > 0:
            table_name = f"{table_name}__{'__'.join(table_name_suffixes)}"
        self.logger.debug(f"Reading {data_name} from {from_date} to {to_date} from {table_name}...")

        if "date" not in cols:
>>>>>>> 1e314e13b6fa1d64fdc5ea31562aa7266bece468
            cols.append("date")
        filters = ""
        if col_filters is None:
            col_filters = {}
        if len(col_filters) > 0:
            filters = []
            for k, v in col_filters.items():
                if isinstance(v, float) or isinstance(v, int):
                    filters.append(f"{k}={v}")
                else:
                    filters.append(f"{k}='{v}'")
            filters = " AND ".join(filters)
            filters = f"AND {filters}"
        if not skip_time_stamps:
            sql = (f"SELECT {', '.join(cols)} FROM "
                f"{table_name} WHERE "
                f"(datetime(date) BETWEEN '{from_date}' AND '{to_date}')"
                f"{filters};")
        elif filters != "" and filters is not None:
            sql = (f"SELECT {', '.join(cols)} FROM "
                f"{table_name} WHERE "
                f"{filters};")
        else:
            sql = (f"SELECT {', '.join(cols)} FROM "
                   f"{table_name}")
        self.logger.debug(f"Executing {sql}")
<<<<<<< HEAD
        with self.write_lock:
            data = self.connection.execute(sql).fetchall()
=======
        data = self.connection.execute(sql).fetchall()
>>>>>>> 1e314e13b6fa1d64fdc5ea31562aa7266bece468
        data = pd.DataFrame(data, columns=cols)
        if index_col is not None:
            data.index = data[index_col]
            data.index.name = index_col
            if index_col == "date":
                data.index = pd.to_datetime(data.index)
            data.drop([index_col], axis=1, inplace=True)
            data = data[~data.index.duplicated(keep='last')]

        return data