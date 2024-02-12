from dataclasses import field
from typing import Union
from dataclasses import field
from typing import Union
import functools
import datetime
import copy
import uuid
import re

import pandas as pd
import numpy as np


def crossunder(df, col1, col2):
    if df.iloc[-2][col1] > df.iloc[-2][col2] and df.iloc[-1][col1] <= df.iloc[-1][col2]:
        return True
    return False

def crossover(df, col1, col2):
    if df.iloc[-2][col1] < df.iloc[-2][col2] and df.iloc[-1][col1] >= df.iloc[-1][col2]:
        return True
    return False

def get_datetime(dt: Union[str, datetime.datetime]):
    if isinstance(dt, str):
        try:
            dt = datetime.datetime.strptime(dt, "%Y%m%d %H:%M")
        except Exception:
            dt = datetime.datetime.strptime(dt, "%Y%m%d")
    return dt
def get_datetime(dt: Union[str, datetime.datetime]):
    if isinstance(dt, str):
        try:
            dt = datetime.datetime.strptime(dt, "%Y%m%d %H:%M")
        except Exception:
            dt = datetime.datetime.strptime(dt, "%Y%m%d")
    return dt

def today_timestamp():
        return datetime.datetime.now().strftime("%Y%m%d")


def datestring_to_datetime(d):
    return datetime.datetime.strptime(d, "%Y%m%d")

def hash_dict(func):
    """Transform mutable dictionnary
    Into immutable
    Useful to be compatible with cache
    """
    class HDict(dict):
        def __hash__(self):
            return hash(frozenset(self.items()))

    class HList(list):
        def __hash__(self):
            return hash(frozenset(self))

    def freeze(arg):
        if isinstance(arg, dict):
            for k, v in arg.items():
                arg[k] = freeze(v)
            return HDict(arg)
        elif isinstance(arg, list):
            return HList([freeze(item) for item in arg])
        return arg


    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        args = tuple([freeze(arg) for arg in args])
        kwargs = {k: freeze(v) for k, v in kwargs.items()}
        return func(*args, **kwargs)
    return wrapped


def new_id():
    return str(uuid.uuid4()).replace("-", "")


def default_dataclass_field(obj):
    """Create a default field"""
    return field(default_factory=lambda: copy.copy(obj))

def current_datetime_field():
    """Create a default field"""
    return field(default_factory=lambda: datetime.datetime.now())

def new_id_field():
    """Create a default field"""
    return field(default_factory=lambda: new_id())


def resample_candle_data(data, interval):
    data = data.resample(interval, origin=datetime.datetime.fromisoformat('1970-01-01 09:15:00')).apply({'open': 'first',
                                          'high': 'max',
                                          'low': 'min',
                                          'close': 'last'})
    data.dropna(inplace=True)
    return data

def sanitize(s: str):
    pattern = re.compile(r"[: \-]")
    return re.sub(pattern, "_", s)


def get_key_from_scrip_and_exchange(scrip: str,
                                    exchange: str):
        scrip = sanitize(scrip)
        exchange = sanitize(exchange)

        return f'{scrip}__{exchange}'

def get_scrip_and_exchange_from_key(key: str):
    if ":" in key:
        parts = key.split(":")
    elif "__" in key:
        parts = key.split("__")
    else:
        raise ValueError(f"No delimiter found in {key} to split it into scrip and exchange")
    return parts


def is_monotonically_increasing(signal: pd.Series):
    return (np.sort(signal.to_numpy()) == signal.to_numpy()).all()


def is_monotonically_decreasing(signal: pd.Series):
    return (np.sort(signal.to_numpy())[::-1] == signal.to_numpy()).all()


def __get_lhs_rhs_pivot(signal: pd.Series, context_size: int):
    lhs = signal.iloc[-(2 * context_size + 1):-(context_size + 1)]
    rhs = signal.iloc[-context_size:]
    pivot = signal.iloc[-(context_size + 1)]
    return lhs, rhs, pivot

def is_local_minima(signal: pd.Series, context_size: int = 1):

    lhs, rhs, pivot = __get_lhs_rhs_pivot(signal, context_size)
    return (is_monotonically_decreasing(lhs)
            and is_monotonically_decreasing(rhs)
            and pivot < lhs.iloc[-1] and pivot < rhs.iloc[-1])


def is_local_maxima(signal: pd.Series, context_size: int = 1):

    lhs, rhs, pivot = __get_lhs_rhs_pivot(signal, context_size)
    return (is_monotonically_increasing(lhs)
            and is_monotonically_decreasing(rhs)
            and pivot > lhs.iloc[-1] and pivot > rhs.iloc[-1])


def candle_body(candle: pd.Series):
    return abs(candle["close"] - candle["open"])


def span(candles: pd.DataFrame,
         size: int = 3,
         direction="down"):
    start_price = 0
    if direction == "down":
        start_price = max(candles.iloc[-size]["close"], candles.iloc[-size]["open"])
        end_price = min(candles.iloc[-size]["close"], candles.iloc[-size]["open"])
    else:
        start_price = min(candles.iloc[-size]["close"], candles.iloc[-size]["open"])
        end_price = max(candles.iloc[-size]["close"], candles.iloc[-size]["open"])

    for ii in range(1, size):
        if direction == "down":
            end_price = min(end_price,
                            min(candles.iloc[-size + ii]["close"],
                                candles.iloc[-size + ii]["open"]))
        else:
            end_price = max(end_price,
                            max(candles.iloc[-size + ii]["close"],
                                candles.iloc[-size + ii]["open"]))
    return abs(start_price - end_price)

def get_pivot_value(signal: pd.Series,
                    context_size=3):
    return signal.iloc[-(context_size + 1)]


def sameday(dt1, dt2):
    if (dt1.day == dt2.day
        and dt1.month == dt2.month
        and dt1.year == dt2.year):
        return True
    return False