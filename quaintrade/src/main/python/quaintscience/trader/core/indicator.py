from abc import ABC, abstractmethod
from typing import Optional, Union
import copy

import pandas as pd
import talib

from .logging import LoggerMixin
from .ds import Order, TradeType, OrderState
from ..integration.common import TradeManager


class Indicator(ABC, LoggerMixin):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def preprocess(self, df: pd.DataFrame,
                output_column_name: Optional[Union[str, list[str]]] = None,
                settings: Optional[dict] = None) -> (pd.DataFrame, Optional[Union[str, list[str]]], Optional[dict]):
        if settings is None:
            settings = {}
        return df, output_column_name, settings

    def postprocess(self, df: pd.DataFrame,
                output_column_name: Optional[Union[str, list[str]]] = None,
                settings: Optional[dict] = None) -> (pd.DataFrame, Optional[Union[str, list[str]]], Optional[dict]):
        for column in output_column_name:
            df[column] = df[column].astype(float)
        return df, output_column_name, settings

    def compute(self, df: pd.DataFrame,
                output_column_name: Optional[Union[str, dict[str, str]]] = None,
                settings: Optional[dict] = None) -> pd.DataFrame:
        df, output_column_name, settings = self.preprocess(df=df,
                                                           output_column_name=output_column_name,
                                                           settings=settings)
        df, output_column_name, settings = self.compute_impl(df=df,
                                                             output_column_name=output_column_name,
                                                             settings=settings)
        df, output_column_name, settings = self.postprocess(df=df,
                                                            output_column_name=output_column_name,
                                                            settings=settings)
        return df, output_column_name, settings

    @abstractmethod
    def compute_impl(self, df: pd.DataFrame,
                output_column_name: Optional[Union[str, dict[str, str]]] = None,
                settings: Optional[dict] = None) -> pd.DataFrame:
        pass


class IndicatorPipeline(Indicator):
    
    def __init__(self,
                 indicators: list[(Indicator, Union[str, dict[str, str]], dict)],
                 *args,
                 **kwargs):
        self.indicators = indicators
        super().__init__(*args, **kwargs)

    def compute_impl(self, df: pd.DataFrame,
                output_column_name: Optional[Union[str, dict[str, str]]] = None,
                settings: Optional[dict] = None) -> pd.DataFrame:
        for indicator, output_column_name, indicator_settings in self.indicators:
            if indicator_settings is None:
                indicator_settings = {}
            indicator_settings = copy.deepcopy(indicator_settings).update(settings)
            self.logger.info(f"Applying {indicator}")
            df, output_column_name, indicator_settings = indicator.compute(df,
                                                                           output_column_name,
                                                                           indicator_settings)
        return df, output_column_name, settings

class DonchainIndicator(Indicator):

    def __init__(self, *args, period: int = 15, **kwargs):
        self.period = period
        super().__init__(*args, **kwargs)

    def compute_impl(self, df: pd.DataFrame,
                output_column_name: Optional[Union[str, dict[str, str]]] = None,
                settings: Optional[dict] = None) -> pd.DataFrame:
        if output_column_name is None:
            output_column_name = ["donchainUpper", "donchainMiddle", "donchainLower"]
        df[output_column_name[0]] = df["high"].rolling(self.period).apply(lambda x: max(x))
        df[output_column_name[2]] = df["low"].rolling(self.period).apply(lambda x: min(x))
        df[output_column_name[1]] = (df[output_column_name[2]] + df[output_column_name[0]]) /2
        return df, output_column_name, settings


class PastPeriodHighLowIndicator(Indicator):

    def __init__(self, *args,
                 period_interval: str = "1d",
                 data_interval: str = "1d",
                 shift: int = 1,
                 **kwargs):
        self.period_interval = period_interval
        self.data_interval = data_interval
        self.shift = shift
        super().__init__(*args, **kwargs)

    def compute_impl(self, df: pd.DataFrame,
                output_column_name: Optional[Union[str, dict[str, str]]] = None,
                settings: Optional[dict] = None) -> pd.DataFrame:

        if output_column_name is None:
            if self.shift == 1:
                output_column_name = [f"p{self.period_interval}h", f"p{self.period_interval}l"]
            elif self.shift == 0:
                output_column_name = [f"c{self.period_interval}h", f"c{self.period_interval}l"]
            else:
                output_column_name = [f"p{self.shift}-{self.period_interval}h", f"p{self.shift}-{self.period_interval}l"]
        
        h = df["high"].resample(self.period_interval).apply("max").shift(self.shift, freq=self.period_interval)
        df[output_column_name[0]] = h.resample(self.data_interval).ffill().ffill()
        pwl = df["low"].resample(self.period_interval).apply("min").shift(self.shift, freq=self.period_interval)
        df[output_column_name[1]] = pwl.resample(self.data_interval).ffill().ffill()
        return df, output_column_name, settings


class SMAIndicator(Indicator):
        
    def __init__(self, *args,
                 period: int = 22,
                 **kwargs):
        self.period = period
        super().__init__(*args, **kwargs)

    def compute_impl(self, df: pd.DataFrame,
                output_column_name: Optional[Union[str, dict[str, str]]] = None,
                settings: Optional[dict] = None) -> pd.DataFrame:
        if output_column_name is None:
            output_column_name = f"SMA{self.period}"
        
        df[output_column_name] = talib.SMA(df[settings.get("input_column", "close")], timeperiod=22)
        return df, output_column_name, settings

class ADXIndicator(Indicator):
        
    def __init__(self, *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

    def compute_impl(self, df: pd.DataFrame,
                output_column_name: Optional[Union[str, dict[str, str]]] = None,
                settings: Optional[dict] = None) -> pd.DataFrame:
        if output_column_name is None:
            output_column_name = ["ADX"]
        df[output_column_name[0]] = talib.ADX(df["high"], df["low"], df["close"])
        return df, output_column_name, settings

class RSIIndicator(Indicator):
        
    def __init__(self, *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

    def compute_impl(self, df: pd.DataFrame,
                output_column_name: Optional[Union[str, dict[str, str]]] = None,
                settings: Optional[dict] = None) -> pd.DataFrame:
        if output_column_name is None:
            output_column_name = ["RSI"]
        
        df[output_column_name[0]] = talib.RSI(df["close"])
        return df, output_column_name, settings


class ATRIndicator(Indicator):
        
    def __init__(self, *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

    def compute_impl(self, df: pd.DataFrame,
                output_column_name: Optional[Union[str, dict[str, str]]] = None,
                settings: Optional[dict] = None) -> pd.DataFrame:
        if output_column_name is None:
            output_column_name = f"ATR"
        
        df[output_column_name] = talib.ATR(df["high"], df["low"], df["close"])
        return df, output_column_name, settings


class BBANDSIndicator(Indicator):
        
    def __init__(self, *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

    def compute_impl(self, df: pd.DataFrame,
                output_column_name: Optional[Union[str, dict[str, str]]] = None,
                settings: Optional[dict] = None) -> pd.DataFrame:
        if output_column_name is None:
            output_column_name = ["BBandUpper", "BBandMiddle", "BBandLower"]
        
        df[output_column_name[0]], df[output_column_name[1]], df[output_column_name[2]] = talib.BBANDS(df["close"])
        return df, output_column_name, settings


class CDLPatternIndicator(Indicator):
        
    def __init__(self, *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

    def compute_impl(self, df: pd.DataFrame,
                     output_column_name: Optional[Union[str, dict[str, str]]] = None,
                     settings: Optional[dict] = None) -> pd.DataFrame:
        if output_column_name is None:
            output_column_name = ""
        
        for item in dir(talib):
            if item.startswith("CDL"):
                df[f"{output_column_name}{item}"] = getattr(talib,
                                                            item)(df["open"],
                                                                  df["high"],
                                                                  df["low"],
                                                                  df["close"])
        return df, output_column_name, settings


class BreakoutIndicator(Indicator):
        
    def __init__(self,
                 upper_breakout_column: str,
                 lower_breakout_column: str,
                 data_interval: str,
                 *args,
                 upper_price_column: str = "high",
                 lower_price_column: str = "low",
                 candle_confirmation: bool = True,
                 **kwargs):
        self.upper_breakout_column = upper_breakout_column
        self.lower_breakout_column = lower_breakout_column
        self.upper_price_column = upper_price_column
        self.lower_price_column = lower_price_column
        self.data_interval = data_interval
        self.candle_confirmation = candle_confirmation
        super().__init__(*args, **kwargs)

    def compute_impl(self, df: pd.DataFrame,
                     output_column_name: Optional[Union[str, dict[str, str]]] = None,
                     settings: Optional[dict] = None) -> pd.DataFrame:
        
        if output_column_name is None or len(output_column_name) == 0:
            output_column_name = [f"{self.upper_breakout_column}_breakout",
                                  f"{self.lower_breakout_column}_breakout"]
        df[output_column_name[0]] = 0.
        df[output_column_name[1]] = 0.
        df.loc[((df[self.upper_price_column] > df[self.upper_breakout_column].shift()) &
                (df[self.upper_price_column].shift() <= df[self.upper_breakout_column].shift(2)) &
                (df["close"] >= df["open"])),
                output_column_name[0]] = 1.0

        df.loc[((df[self.lower_price_column] < df[self.lower_breakout_column].shift()) &
                (df[self.lower_price_column].shift() >= df[self.lower_breakout_column].shift(2)) &
                (df["close"] <= df["open"])),
                output_column_name[1]] = 1.0
        return df, output_column_name, settings
