from abc import abstractmethod
import datetime
from typing import Union

import pandas as pd


class OHLCStorageMixin():

    def __init__(self,
                 *args, **kwargs):
        pass
    
    @abstractmethod
    def put(self, scrip: str, exchange: str, df: pd.DataFrame):
        pass

    @abstractmethod
    def get(self, scrip: str, exchange: str,
            fromdate: Union[str, datetime.datetime],
            todate: Union[str, datetime.datetime]) -> pd.DataFrame:
        pass
