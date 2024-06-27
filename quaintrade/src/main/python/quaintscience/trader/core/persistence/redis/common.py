from abc import abstractmethod, ABC
from typing import Union, Optional
import redis
import datetime

import pandas as pd

from ..common import Storage
from ...util import sanitize, get_datetime


class RedisStorage(Storage):

    def __init__(self, *args, **kwargs):
        self.cache = {}
        super().__init__(*args, **kwargs)

    def connect(self):
        self.logger.debug(f"Connecting to {self.path}")
        self.connection = redis.Redis(host=self.path.split(":")[0], port=int(self.path.split(":")[1]), decode_responses=True)

    def store_ts_data(self, table_name, df: pd.DataFrame) -> None:
        ts = self.connection.ts()
        if self.connection.exists(f"{table_name}:O") == 0:
            ts.create(f"{table_name}:O")
        if self.connection.exists(f"{table_name}:H") == 0:
            ts.create(f"{table_name}:H")
        if self.connection.exists(f"{table_name}:L") == 0:
            ts.create(f"{table_name}:L")
        if self.connection.exists(f"{table_name}:C") == 0:
            ts.create(f"{table_name}:C")
        
        


    def commit(self):
        pass