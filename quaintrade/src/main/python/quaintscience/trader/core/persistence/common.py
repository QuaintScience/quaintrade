from abc import abstractmethod, ABC
from typing import Union, Optional
import sqlite3
import datetime

import pandas as pd

from ..logging import LoggerMixin
from ..util import get_datetime, sanitize


class Storage(ABC, LoggerMixin):
    
    def __init__(self,
                 path: str,
                 *args, **kwargs):
        self.path = path
        super().__init__(*args, **kwargs)
        self.connect()

    @abstractmethod
    def connect(self):
        pass


    @abstractmethod
    def commit(self):
        pass
