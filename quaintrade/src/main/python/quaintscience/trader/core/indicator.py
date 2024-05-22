from abc import ABC, abstractmethod
from typing import Optional, Union, Tuple
import datetime
import copy
import datetime
import numpy as np
import pandas as pd
import pandas_ta as pd_ta
import pandas_ta as pd_ta
import talib

from .logging import LoggerMixin


class Indicator(ABC, LoggerMixin):

    def __init__(self, *args,
                 setting_attrs: Optional[list[str]] = None,
                 **kwargs):
        self.setting_attrs = setting_attrs if setting_attrs is not None else []
    def __init__(self, *args,
                 setting_attrs: Optional[list[str]] = None,
                 **kwargs):
        self.setting_attrs = setting_attrs if setting_attrs is not None else []
        super().__init__(*args, **kwargs)
    
    def add_setting(self, settings, name):
        settings[name] = settings.get(name, getattr(self, name))

    def is_red_candle(self, opn, close=None):
        is_shooting_star = False
        is_hanging_man = False
        if close is None:
            if "CDLSHOOTINGSTAR" in opn:
                is_shooting_star = True if opn["CDLSHOOTINGSTAR"] != 0 else False
            if "CDLHANGINGMAN" in opn:
                is_hanging_man = True if opn["CDLHANGINGMAN"] != 0 else False
            opn, close = opn["open"], opn["close"]
        if opn > close or is_shooting_star or is_hanging_man:
            return True
        return False

    def is_green_candle(self, opn, close=None):
        return not self.is_red_candle(opn, close)

    def is_doji(self, opn, high=None, low=None, close=None, settings=None):
        is_cdl_doji = False
        if settings is None:
            wick_threshold = getattr(self, "wick_threshold", 2.0)
        else:
            wick_threshold = settings.get("wick_threshold", getattr(self, "wick_threshold", 2.0))
        if high is None:
            if "CDLDOJI" in opn:
                is_cdl_doji = True if opn["CDLDOJI"] != 0 else False
            opn, high, low, close = opn["open"], opn["high"], opn["low"], opn["close"]
        return (not (abs(opn - close) / (high - max(close, opn)) >= wick_threshold or abs(opn - close) / (min(opn, close) - low) >= wick_threshold)) or  is_cdl_doji
    
    def add_setting(self, settings, name):
        settings[name] = settings.get(name, getattr(self, name))

    def is_red_candle(self, opn, close=None):
        is_shooting_star = False
        is_hanging_man = False
        if close is None:
            if "CDLSHOOTINGSTAR" in opn:
                is_shooting_star = True if opn["CDLSHOOTINGSTAR"] != 0 else False
            if "CDLHANGINGMAN" in opn:
                is_hanging_man = True if opn["CDLHANGINGMAN"] != 0 else False
            opn, close = opn["open"], opn["close"]
        if opn > close or is_shooting_star or is_hanging_man:
            return True
        return False

    def is_green_candle(self, opn, close=None):
        return not self.is_red_candle(opn, close)

    def is_doji(self, opn, high=None, low=None, close=None, settings=None):
        is_cdl_doji = False
        if settings is None:
            wick_threshold = getattr(self, "wick_threshold", 2.0)
        else:
            wick_threshold = settings.get("wick_threshold", getattr(self, "wick_threshold", 2.0))
        if high is None:
            if "CDLDOJI" in opn:
                is_cdl_doji = True if opn["CDLDOJI"] != 0 else False
            opn, high, low, close = opn["open"], opn["high"], opn["low"], opn["close"]
        return (not (abs(opn - close) / (high - max(close, opn)) >= wick_threshold or abs(opn - close) / (min(opn, close) - low) >= wick_threshold)) or  is_cdl_doji

    def get_default_settings(self, settings: Optional[dict] = None):
        if settings is None:
            settings = {}
        else:
            settings = settings.copy()
        for attr in self.setting_attrs:
            self.add_setting(settings, attr)

        for attr in self.setting_attrs:
            self.add_setting(settings, attr)
        return settings

    def get_default_column_names(self,
                                 output_column_names: dict[str, str] | None = None,
                                 settings: dict | None = None) -> dict[str, str]:
        if output_column_names is None:
            output_column_names = {}
        if settings is None:
            settings = self.get_default_settings()

        return self.get_default_column_names_impl(output_column_names.copy(), settings)

    @abstractmethod
    def get_default_column_names_impl(self,
                                 output_column_names: dict[str, str],
                                 settings: dict) -> dict[str, str]:
        pass

    def preprocess(self, df: pd.DataFrame,
                   output_column_names: Optional[Union[str, list[str]]] = None,
                   settings: Optional[dict] = None) -> Tuple[pd.DataFrame,
                                                             Optional[Union[str, list[str]]],
                                                             Optional[dict]]:
        settings = self.get_default_settings(settings)
        output_column_names = self.get_default_column_names(output_column_names=output_column_names)
        return df, output_column_names, settings

    def postprocess(self, df: pd.DataFrame,
                output_column_names: dict[str, str],
                settings: dict) -> Tuple[pd.DataFrame,
                                         Optional[Union[str, list[str]]],
                                         Optional[dict]]:
        for _, column_name in output_column_names.items():
            df[column_name] = df[column_name].astype(float)
        for _, column_name in output_column_names.items():
            df[column_name] = df[column_name].astype(float)
        return df

    def compute(self, df: pd.DataFrame,
                output_column_names: Optional[Union[str, dict[str, str]]] = None,
                settings: Optional[dict] = None) -> pd.DataFrame:
        df, output_column_names, settings = self.preprocess(df=df,
                                                           output_column_names=output_column_names,
                                                           settings=settings)
        df = self.compute_impl(df=df,
                               output_column_names=output_column_names,
                               settings=settings)
        if isinstance(df, tuple):
            df, output_column_names, settings = df
        df = self.postprocess(df=df,
                              output_column_names=output_column_names,
                              settings=settings)
        return df, output_column_names, settings

    @abstractmethod
    def compute_impl(self, df: pd.DataFrame,
                output_column_names: dict[str, str],
                settings: dict) -> pd.DataFrame:
        pass


class IndicatorPipeline(Indicator):
    
    def __init__(self,
                 indicators: list[(Indicator, Union[str, dict[str, str]], dict)],
                 *args,
                 **kwargs):
        self.indicators = indicators
        super().__init__(*args, **kwargs)

    def get_default_column_names_impl(self,
                                      output_column_names: dict[str, str],
                                      settings: dict) -> dict[str, str]:
        return output_column_names

    def compute_impl(self, df: pd.DataFrame,
                output_column_names: dict[str, str],
                settings: dict) -> pd.DataFrame:
        ret_settings = {}
        ret_col_names = {}
        for indicator, ind_output_column_names, indicator_settings in self.indicators:
            if indicator_settings is None:
                indicator_settings = {}
            indicator_settings = copy.deepcopy(indicator_settings).update(settings)
            self.logger.info(f"Applying {indicator.__class__.__name__}")
            (df,
             indicator_output_column_names,
             indicator_settings) = indicator.compute(df,
                                                     ind_output_column_names,
                                                     indicator_settings)
            ret_col_names.update(indicator_output_column_names)
            ret_settings.update(indicator_settings)
        return df, ret_col_names, ret_settings


class SlopeIndicator(Indicator):
    def __init__(self,
                 signal: str,
                 *args,
                 shift: int = 1,
                 **kwargs):
        self.signal = signal
        self.shift = shift
        kwargs["setting_attrs"] = ["signal", "shift"]
        super().__init__(*args, **kwargs)
    
    def get_default_column_names_impl(self,
                                     output_column_names: dict[str, str],
                                     settings: dict) -> dict[str, str]:

        output_column_names["slope"] = f"{settings['signal']}_slope"
        output_column_names["acceleration"] = f"{settings['signal']}_acceleration"
        return output_column_names

    def compute_impl(self, df: pd.DataFrame,
                     output_column_names: dict[str, str],
                     settings: dict) -> pd.DataFrame:
            
        df[output_column_names["slope"]] = df[settings['signal']].diff(periods=settings['shift'])
        df[output_column_names["acceleration"]] = df[settings['signal']].diff(settings['shift']).diff()
        return df


class DonchianIndicator(Indicator):

    def __init__(self, *args, period: int = 15, **kwargs):
        self.period = period
        kwargs["setting_attrs"] = ["period"]
        super().__init__(*args, **kwargs)

    def get_default_column_names_impl(self,
                                      output_column_names: dict[str, str],
                                      settings: dict) -> dict[str, str]:
        x = ["upper", "basis", "lower"]
        y = [f"donchian_{val}_{settings['period']}" for val in x]
        output_column_names.update(dict(zip(x, y)))
        return output_column_names

    def compute_impl(self, df: pd.DataFrame,
                output_column_names: dict[str, str],
                settings: dict) -> pd.DataFrame:
            
        df[output_column_names["upper"]] = df["high"].rolling(settings['period']).apply(lambda x: max(x))
        df[output_column_names["lower"]] = df["low"].rolling(settings['period']).apply(lambda x: min(x))
        df[output_column_names["basis"]] = (df[output_column_names["upper"]] + df[output_column_names["lower"]]) / 2
        return df


class PullbackDetector(Indicator):

    PULLBACK_DIRECTION_UP = "up"
    PULLBACK_DIRECTION_DOWN = "down"

    def __init__(self,
                 breakout_column: str,
                 price_column: str,
                 pullback_direction: str,
                 *args,
                 data_period: str = "10min",
                 wick_threshold: float = 2.,
                 **kwargs):
        self.price_column = price_column
        self.breakout_column = breakout_column
        self.pullback_direction = pullback_direction
        self.wick_threshold = wick_threshold
        self.data_period = data_period
        kwargs["setting_attrs"] = ["price_column",
                                   "breakout_column",
                                   "pullback_direction",
                                   "wick_threshold",
                                   "data_period"]
        super().__init__(*args,
                         **kwargs)


    def get_default_column_names_impl(self,
                                      output_column_names: dict[str, str],
                                      settings: dict) -> dict[str, str]:
        x = ["pullback_start", "pullback_end"]
        y = [f"{settings['breakout_column']}_{val}" for val in x]
        output_column_names.update(dict(zip(x, y)))
        return output_column_names

    def compute_impl(self, df: pd.DataFrame,
                     output_column_names: dict[str, str],
                     settings: dict) -> pd.DataFrame:
           
        after_breakout = False
        pull_back_in_progress = False

        df["_breakouts"] = 0.0
        if settings["pullback_direction"] == PullbackDetector.PULLBACK_DIRECTION_DOWN:
            df.loc[df[settings["price_column"]] >= df[settings["breakout_column"]], "_breakouts"] = 1.0
        else:
            df.loc[df[settings["price_column"]] <= df[settings["breakout_column"]], "_breakouts"] = 1.0

        prev_row = None
        for v in output_column_names.values():
            df[v] = 0.
        for _, row in df.iterrows():
            if settings["pullback_direction"] == PullbackDetector.PULLBACK_DIRECTION_DOWN:
                if (prev_row is not None
                    and row["_breakouts"] != 1.0
                    and prev_row["_breakouts"] == 1.0):
                        df.loc[row.name, output_column_names["pullback_start"]] = 1.0
                        if self.is_red_candle(row):
                            pull_back_in_progress = True
                            after_breakout = False
                        else:
                            after_breakout = True
                            pull_back_in_progress = False
                        prev_row = row
                        continue
                if after_breakout:
                    if (self.is_red_candle(row)
                        and not self.is_doji(row, settings=settings)):
                        pull_back_in_progress = True
                        after_breakout = False
                elif pull_back_in_progress:
                    if self.is_green_candle(row):
                        df.loc[row.name, output_column_names["pullback_end"]] = 1.0
                        pull_back_in_progress = False
                        after_breakout = False
                prev_row = row
            if settings["pullback_direction"] == PullbackDetector.PULLBACK_DIRECTION_UP:
                if (prev_row is not None
                    and row["_breakouts"] != 1.0
                    and prev_row["_breakouts"] == 1.0):
                        df.loc[row.name, output_column_names["pullback_start"]] = 1.0
                        if self.is_green_candle(row):
                            pull_back_in_progress = True
                            after_breakout = False
                        else:
                            after_breakout = True
                            pull_back_in_progress = False
                        prev_row = row
                        continue
                if after_breakout:
                    if (self.is_green_candle(row)
                        and not self.is_doji(row, settings=settings)):
                        pull_back_in_progress = True
                        after_breakout = False
                elif pull_back_in_progress:
                    if self.is_red_candle(row):
                        df.loc[row.name, output_column_names["pullback_end"]] = 1.0
                        pull_back_in_progress = False
                        after_breakout = False
                prev_row = row

        return df


class PastPeriodHighLowIndicator(Indicator):

    def __init__(self, *args,
                 period_interval: str = "1d",
                 data_interval: str = "1d",
                 shift: int = 1,
                 **kwargs):
        self.period_interval = period_interval
        self.data_interval = data_interval
        self.shift = shift
        kwargs["setting_attrs"] = ["period_interval", "data_interval", "shift"]
        super().__init__(*args, **kwargs)

    def get_default_column_names_impl(self,
                                      output_column_names: dict[str, str],
                                      settings: dict) -> dict[str, str]:
        x = ["previous_high", "previous_low"]
        y = [f"{val}_{settings['period_interval']}_{settings['shift']}" for val in x]
        output_column_names.update(dict(zip(x, y)))
        return output_column_names

    def compute_impl(self, df: pd.DataFrame,
                     output_column_names: dict[str, str],
                     settings: dict) -> pd.DataFrame:

        pwh = df["high"].resample(settings["period_interval"],
                                  origin=datetime.datetime.fromisoformat('1970-01-01 09:15:00')).apply("max").shift(settings["shift"],
                                                                                  freq=settings["period_interval"])
        df[output_column_names["previous_high"]] = pwh.resample(settings["data_interval"],
                                                                origin=datetime.datetime.fromisoformat('1970-01-01 09:15:00')).ffill().ffill()
        pwl = df["low"].resample(settings["period_interval"],
                                 origin=datetime.datetime.fromisoformat('1970-01-01 09:15:00')).apply("min").shift(settings["shift"],
                                                                                 freq=settings["period_interval"])
        df[output_column_names["previous_low"]] = pwl.resample(settings["data_interval"],
                                                                        origin=datetime.datetime.fromisoformat('1970-01-01 09:15:00')).ffill().ffill()
        return df

class PauseBarIndicator(Indicator):

    def __init__(self, *args,
                 atr_threshold: float = 0.4,
                 atr_column_name: str = "ATR_14",
                 **kwargs):
        self.atr_threshold = atr_threshold
        self.atr_column_name = atr_column_name
        kwargs["setting_attrs"] = ["atr_threshold", "atr_column_name"]
        super().__init__(*args, **kwargs)

    def get_default_column_names_impl(self,
                                      output_column_names: dict[str, str],
                                      settings: dict) -> dict[str, str]:
        output_column_names.update({'is_pause': f"is_pause_{settings['atr_threshold']:.2f}_{settings['atr_column_name']}"})
        return output_column_names

    def compute_impl(self, df: pd.DataFrame,
                     output_column_names: dict[str, str],
                     settings: dict) -> pd.DataFrame:

        df[output_column_names['is_pause']] = 0.
        df.loc[((df["close"] - df["open"]).abs() < (df[settings['atr_column_name']] * settings['atr_threshold'])), output_column_names['is_pause']] = 1.0
        return df


class IntradayHighLowIndicator(Indicator):

    def __init__(self, *args,
                 start_hour: int,
                 start_minute: int,
                 end_hour: int,
                 end_minute: int,
                 **kwargs):
        self.start_hour = start_hour
        self.end_hour = end_hour
        self.start_minute = start_minute
        self.end_minute = end_minute
        kwargs["setting_attrs"] = ["start_hour",
                                   "start_minute",
                                   "end_hour",
                                   "end_minute"]
        super().__init__(*args, **kwargs)

    def get_default_column_names_impl(self,
                                      output_column_names: dict[str, str],
                                      settings: dict) -> dict[str, str]:
        output_column_names.update({"period_high": f"period_high_{settings['start_hour']}_{settings['start_minute']}_{settings['end_hour']}_{settings['end_minute']}",
                                    "period_low": f"period_low_{settings['start_hour']}_{settings['start_minute']}_{settings['end_hour']}_{settings['end_minute']}"})
        return output_column_names

    def compute_impl(self, df: pd.DataFrame,
                     output_column_names: dict[str, str],
                     settings: dict) -> pd.DataFrame:


        df[output_column_names["period_high"]] = np.nan
        df[output_column_names["period_low"]] = np.nan

        current_high = np.nan
        current_low = np.nan
        
        for row_id, row in df.iterrows():
            if (row_id.hour < settings["start_hour"]
                or (row_id.hour == settings["start_hour"] and row_id.minute < settings["start_minute"])):
                current_high = np.nan
                current_low = np.nan
                continue
            if (((row_id.hour == settings["start_hour"] and row_id.minute >= settings["start_minute"])
                 or row_id.hour > settings["start_hour"])
                 and (row_id.hour < settings["end_hour"] or
                      (row_id.hour == settings["end_hour"] and row_id.minute <= settings["end_hour"]))):
                if np.isnan(current_high):
                    current_high = row["high"]
                else:
                    current_high = max(current_high, row["high"])
                if np.isnan(current_low):
                    current_low = row["low"]
                else:
                    current_low = min(current_low, row["low"])
            df.loc[row_id, output_column_names["period_high"]] = current_high
            df.loc[row_id, output_column_names["period_low"]] = current_low

        return df


class MAIndicator(Indicator):
        
    def __init__(self, *args,
                 period: int = 22,
                 signal: str = "close",
                 ma_type: str = "SMA",
                 **kwargs):
        self.period = period
        self.signal = signal
        self.ma_type = ma_type
        kwargs["setting_attrs"] = ["period", "signal", "ma_type"]
        super().__init__(*args, **kwargs)

    def get_default_column_names_impl(self,
                                      output_column_names: dict[str, str],
                                      settings: dict) -> dict[str, str]:
        output_column_names.update({"MA": f"{settings['ma_type']}_{settings['signal']}_{settings['period']}"})
        return output_column_names

    def compute_impl(self, df: pd.DataFrame,
                output_column_names: dict[str, str],
                settings: dict) -> pd.DataFrame:
        df[output_column_names["MA"]] = getattr(talib, settings["ma_type"])(df[settings['signal']],
                                                                            timeperiod=self.period)
        return df


class ADXIndicator(Indicator):

    def __init__(self, *args,
                 period: int = 14,
                 **kwargs):
        self.period = period
        kwargs["setting_attrs"] = ["period"]
        super().__init__(*args, **kwargs)

    def get_default_column_names_impl(self,
                                output_column_names: dict[str, str],
                                settings: dict) -> dict[str, str]:

        output_column_names.update({"ADX": f"ADX_{settings['period']}"})
        return output_column_names

    def compute_impl(self, df: pd.DataFrame,
                     output_column_names: dict[str, str],
                     settings: dict) -> pd.DataFrame:
        df[output_column_names["ADX"]] = talib.ADX(df["high"], df["low"], df["close"], timeperiod=settings['period'])
        return df
    

class ATRIndicator(Indicator):

    def __init__(self, *args,
                 period: int = 14,
                 **kwargs):
        self.period = period
        kwargs["setting_attrs"] = ["period"]
        super().__init__(*args, **kwargs)

    def get_default_column_names_impl(self,
                                      output_column_names: dict[str, str],
                                      settings: dict) -> dict[str, str]:
        output_column_names.update({"ATR": f"ATR_{settings['period']}"})
        return output_column_names

    def compute_impl(self, df: pd.DataFrame,
                output_column_names: dict[str, str],
                settings: dict) -> pd.DataFrame:
        df[output_column_names["ATR"]] = talib.ATR(df["high"], df["low"], df["close"], timeperiod=settings['period'])
        return df


class RSIIndicator(Indicator):
        
    def __init__(self,
                 *args,
                 period: int = 14,
                 **kwargs):
        self.period = period
        kwargs["setting_attrs"] = ["period"]
        super().__init__(*args, **kwargs)

    def get_default_column_names_impl(self,
                                output_column_names: dict[str, str],
                                settings: dict) -> dict[str, str]:

        output_column_names.update({"RSI": f"RSI_{settings['period']}"})
        return output_column_names

    def compute_impl(self, df: pd.DataFrame,
                output_column_names: dict[str, str],
                settings: dict) -> pd.DataFrame:
        
        df[output_column_names["RSI"]] = talib.RSI(df["close"], timeperiod=settings['period'])
        return df


class ChoppinessIndicator(Indicator):
        
    def __init__(self,
                 *args,
                 period: int = 14,
                 atr_length: int = 1,
                 drift: int = 1,
                 **kwargs):
        self.period = period
        self.atr_length = atr_length
        self.drift = drift
        kwargs["setting_attrs"] = ["period", "atr_length", "drift"]
        super().__init__(*args, **kwargs)

    def get_default_column_names_impl(self,
                                      output_column_names: dict[str, str],
                                      settings: dict) -> dict[str, str]:
        output_column_names.update({"choppiness": f"choppiness_{settings['period']}"})
        return output_column_names

    def compute_impl(self, df: pd.DataFrame,
                output_column_names: dict[str, str],
                settings: dict) -> pd.DataFrame:
        df[output_column_names["choppiness"]] = pd_ta.chop(high=df["high"], low=df["low"], close=df["close"],
                                                          length=settings['period'],
                                                          atr_length=settings['atr_length'],
                                                          drift=settings['drift'])
        return df
    

class SupertrendIndicator(Indicator):
        
    def __init__(self,
                 *args,
                 period: int = 7,
                 multiplier: float = 3.0,
                 **kwargs):
        self.period = period
        self.multiplier = multiplier
        kwargs["setting_attrs"] = ["period", "multiplier"]
        super().__init__(*args, **kwargs)

    def get_default_column_names_impl(self,
                                      output_column_names: dict[str, str],
                                      settings: dict) -> dict[str, str]:

        output_column_names.update({"supertrend": f"supertrend_{settings['period']}_{settings['multiplier']:.1f}"})
        return output_column_names

    def compute_impl(self, df: pd.DataFrame,
                output_column_names: dict[str, str],
                settings: dict) -> pd.DataFrame:

        result = pd_ta.supertrend(high=df["high"],
                                  low=df["low"],
                                  close=df["close"],
                                  length=settings['period'],
                                  multiplier=settings['multiplier'])
        df[output_column_names["supertrend"]] = pd.NA
        if result is not None:
            df[output_column_names["supertrend"]] = result[f"SUPERT_{settings['period']}_{settings['multiplier']:.1f}"]
        df[output_column_names["supertrend"]].fillna(df["close"].mean(), inplace=True)
        df.loc[df[output_column_names["supertrend"]] < 1e-3, output_column_names["supertrend"]] = df["close"].mean()
        return df
    

class BBANDSIndicator(Indicator):
        
    def __init__(self,
                 *args,
                 period: int = 5,
                 nbdevup: float = 2.0,
                 nbdevdown: float = 2.0,
                 **kwargs):
        self.period = period
        self.nbdevup = nbdevup
        self.nbdevdown = nbdevdown
        kwargs["setting_attrs"] = ["period", "nbdevup", "nbdevdown"]
        super().__init__(*args, **kwargs)

    def get_default_column_names_impl(self,
                                 output_column_names: dict[str, str],
                                 settings: dict) -> dict[str, str]:
        output_column_names.update({"supertrend": f"supertrend_{settings['period']}_{settings['multiplier']:.1f}"})
        x = ["BBandUpper", "BBandMiddle", "BBandLower"]
        y = [f"{val}_{settings['period']}" for val in x]
        output_column_names.update(dict(zip(x, y)))
        return output_column_names

    def compute_impl(self, df: pd.DataFrame,
                     output_column_name: dict[str, str],
                     settings: dict = None) -> pd.DataFrame:
        if settings is None:
            settings = self.get_default_settings()
        (df[output_column_name["BBandUpper"]],
         df[output_column_name["BBandMiddle"]],
         df[output_column_name["BBandLower"]]) = talib.BBANDS(df["close"],
                                                              timeperiod=settings['period'],
                                                              nbdevup=settings['nbdevup'],
                                                              nbdevdn=settings['nbdevdown'])
        return df


class CDLPatternIndicator(Indicator):
        
    def __init__(self, *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

    def get_default_column_names_impl(self,
                                      output_column_names: dict[str, str],
                                      settings: dict) -> dict[str, str]:

        output_column_names.update({"supertrend": f"supertrend_{settings['period']}_{settings['multiplier']:.1f}"})
        for item in dir(talib):
            if item.startswith("CDL"):
                output_column_names[item] = item
        return output_column_names

    def compute_impl(self, df: pd.DataFrame,
                     output_column_names: dict[str, str],
                     settings: dict = None) -> pd.DataFrame:
        if settings is None:
            settings = self.get_default_settings()
        for item in output_column_names.keys():
            df[item] = getattr(talib, item)(df["open"], df["high"], df["low"], df["close"])
        return df


class MajorityRuleIndicator(Indicator):

    def __init__(self, *args,
                 period: int = 14,
                 **kwargs):
        self.period = period
        kwargs["setting_attrs"] = ["period"]
        super().__init__(self, *args, **kwargs)
    
    def get_default_column_names_impl(self,
                                      output_column_names: dict[str, str],
                                      settings: dict) -> dict[str, str]:

        output_column_names.update({"majority_rule": f"majority_rule_{settings['period']}"})
        return output_column_names

    def compute_impl(self, df: pd.DataFrame,
                     output_column_names: dict[str, str],
                     settings: dict) -> pd.DataFrame:

        res = (df["close"] - df["open"] > 0).rolling(window=settings["period"]).sum()
        res = res / settings["period"]
        #res[np.isnan(res)] = 0.
        #res[np.isinf(res)] = 1.
        df[output_column_names["majority_rule"]] = res
        return df


class IchimokuIndicator(Indicator):

    def __init__(self,
                 *args,
                 tenkan_period: int = 9,
                 kijun_period: int = 26,
                 senkou_span_b_period: int = 52,
                 chikou_offset: int = -22,
                 **kwargs):
        self.tenkan_period = tenkan_period
        self.kijun_period = kijun_period
        self.senkou_span_b_period = senkou_span_b_period
        self.chikou_offset = chikou_offset
        kwargs["setting_attrs"] = ["tenkan_period", "kijun_period",
                                   "senkou_span_b_period", "chikou_offset"]
        super().__init__(*args, **kwargs)
    
    def get_default_column_names_impl(self,
                                      output_column_names: dict[str, str],
                                      settings: dict) -> dict[str, str]:

        output_column_names.update({"tenkan_sen": "tenkan_sen",
                                    "kijun_sen": "kijun_sen",
                                    "senkou_span_a": "senkou_span_a",
                                    "senkou_span_b": "senkou_span_b",
                                    "chikou_span": "chikou_span"})
        return output_column_names

    def compute_impl(self, df: pd.DataFrame,
                     output_column_name: dict[str, str],
                     settings: dict) -> pd.DataFrame:
        # Tenkan-sen (Conversion Line): (9-period high + 9-period low)/2))
        period9_high = df["high"].rolling(window=settings["tenkan_period"]).max()
        period9_low = df["low"].rolling(window=settings["tenkan_period"]).min()
        tenkan_sen = (period9_high + period9_low) / 2

        # Kijun-sen (Base Line): (26-period high + 26-period low)/2))
        period26_high = df["high"].rolling(window=settings["kijun_period"]).max()
        period26_low = df["low"].rolling(window=settings["kijun_period"]).min()
        kijun_sen = (period26_high + period26_low) / 2

        # Senkou Span A (Leading Span A): (Conversion Line + Base Line)/2))
        senkou_span_a = ((tenkan_sen + kijun_sen) / 2).shift(26)

        # Senkou Span B (Leading Span B): (52-period high + 52-period low)/2))
        period52_high = df["high"].rolling(window=settings["senkou_span_b_period"]).max()
        period52_low = df["low"].rolling(window=settings["senkou_span_b_period"]).min()
        senkou_span_b = ((period52_high + period52_low) / 2).shift(26)

        # The most current closing price plotted 22 time periods behind (optional)
        chikou_span = df["close"].shift(settings["chikou_offset"]) # 22 according to investopedia

        df["tenkan_sen"] = tenkan_sen
        df["kijun_sen"] = kijun_sen
        df["senkou_span_a"] = senkou_span_a
        df["senkou_span_b"] = senkou_span_b
        df["chikou_span"] = chikou_span

        return df


class PivotIndicator(Indicator):

    def __init__(self,
                 *args,
                 left_period: int = 10,
                 right_period: int = 10,
                 **kwargs):
        self.left_period = left_period
        self.right_period = right_period
        kwargs["setting_attrs"] = ["left_period", "right_period"]
        super().__init__(*args, **kwargs)

    def get_default_column_names_impl(self,
                                      output_column_names: dict[str, str],
                                      settings: dict) -> dict[str, str]:
        dct = {}
        if settings["left_period"] != settings["right_period"]:
            dct = {"pivot_high": f"pivot_high_{settings['left_period']}_{settings['right_period']}",
                    "pivot_low": f"pivot_low_{settings['left_period']}_{settings['right_period']}"}
        else:
            dct = {"pivot_high": f"pivot_high_{settings['left_period']}",
                    "pivot_low": f"pivot_low_{settings['left_period']}"}
        output_column_names.update(dct)
        return output_column_names

    def compute_impl(self, df: pd.DataFrame,
                     output_column_names: dict[str, str],
                     settings: dict) -> pd.DataFrame:

        df[output_column_names["pivot_high"]] = df["high"].shift(-settings["right_period"], fill_value=0).rolling(settings["left_period"]).max()
        df[output_column_names["pivot_low"]] = df["low"].shift(-settings["right_period"], fill_value=0).rolling(settings["left_period"]).min()
        print(df)
        return df


class GapUpDownIndicator(Indicator):

    def __init__(self,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

    def get_default_column_names_impl(self,
                                      output_column_names: dict[str, str],
                                      settings: dict) -> dict[str, str]:
        output_column_names.update({"gapup": "gapup",
                                    "gapdown": "gapdown"})
        return output_column_names

    def compute_impl(self, df: pd.DataFrame,
                     output_column_names: dict[str, str],
                     settings: dict) -> pd.DataFrame:
        
        sh = df["high"].shift()
        sl = df["low"].shift()
        df[output_column_names["gapup"]] = 0.
        df[output_column_names["gapdown"]] = 0.
        df.loc[(df.index.minute == 15) & (df.index.hour == 9) & (sh < df["low"]), output_column_names["gapup"]] = 1.0
        df.loc[(df.index.minute == 15) & (df.index.hour == 9) & (sl > df["high"]), output_column_names["gapdown"]] = 1.0
        return df


class HeikinAshiIndicator(Indicator):

    def __init__(self, *args,
                 replace_ohlc: bool = False,
                 **kwargs):
        self.replace_ohlc = replace_ohlc
        kwargs["setting_attrs"] = ["replace_ohlc"]
        super().__init__(*args, **kwargs)

    def get_default_column_names_impl(self,
                                      output_column_names: dict[str, str],
                                      settings: dict) -> dict[str, str]:
        output_column_names.update({"ha_open": "ha_open",
                                    "ha_close": "ha_close",
                                    "ha_high": "ha_high",
                                    "ha_low": "ha_low",
                                    "ha_bullsish": "ha_bullish",
                                    "ha_bearish": "ha_bearish",
                                    "ha_doji": "ha_doji"})
        return output_column_names

    def compute_impl(self, df: pd.DataFrame,
                     output_column_names: dict[str, str],
                     settings: dict) -> pd.DataFrame:


        heikin_ashi_df = pd.DataFrame(index=df.index.values,
                                      columns=['open',
                                               'high',
                                               'low',
                                               'close',
                                               'volume',
                                               'oi'])
    
        heikin_ashi_df['close'] = (df['open'] + df['high'] + df['low'] + df['close']) / 4
        #heikin_ashi_df['close'] = df["close"]
        
        for i in range(len(df)):
            if i == 0:
                heikin_ashi_df.iat[0, 0] = df['open'].iloc[0]
            else:
                heikin_ashi_df.iat[i, 0] = (heikin_ashi_df.iat[i-1, 0] + heikin_ashi_df.iat[i-1, 3]) / 2
            
        heikin_ashi_df['high'] = heikin_ashi_df.loc[:, ['open', 'close']].join(df['high']).max(axis=1)
        
        heikin_ashi_df['low'] = heikin_ashi_df.loc[:, ['open', 'close']].join(df['low']).min(axis=1)
        
        for col in ["open", "high", "low", "close"]:
            df[f"ha_{col}"] = heikin_ashi_df[col].astype(float)
            if settings['replace_ohlc']:
                df[col] = heikin_ashi_df[col].astype(float)

        df["ha_bullish"] = 0.
        df.loc[(df["ha_close"] > df["ha_open"]) & (df["ha_open"] == df["ha_low"]), "ha_bullish"] = 1.0
        df["ha_bearish"] = 0.
        df.loc[(df["ha_close"] < df["ha_open"]) & (df["ha_open"] == df["ha_high"]), "ha_bearish"] = 1.0
        df["ha_doji"] = 0.
        
        upper_wick = df["ha_high"] - df[["ha_open", "ha_close"]].max(axis=1)
        lower_wick = df[["ha_open", "ha_close"]].min(axis=1) - df["ha_low"]
        body = (df["ha_close"] - df["ha_open"]).abs()
        df.loc[(((upper_wick > body) | (lower_wick > body))
                 & (upper_wick > 0) & (lower_wick > 0)),
               "ha_doji"] = 1.0

        return df


class SupportIndicator(Indicator):

    SUPPORT_DIRECTION_UP = "up"
    SUPPORT_DIRECTION_DOWN = "down"

    def __init__(self,
                 direction: str,
                 support_signal: str,
                 signal: str,
                 *args,
                 factor: float = 0.04/100,
                 **kwargs):
        self.direction = direction
        self.support_signal = support_signal
        self.signal = signal
        self.factor = factor
        kwargs['setting_attrs'] = ["direction",
                                   "support_signal",
                                   "signal",
                                   "factor"]
        super().__init__(*args, **kwargs)

    def get_default_column_names_impl(self,
                                      output_column_names: dict[str, str],
                                      settings: dict) -> dict[str, str]:
        output_column_names.update({"support": f"support_{settings['direction']}_of_{settings['support_signal']}_by{settings['signal']}"})
        return output_column_names

    def compute_impl(self, df: pd.DataFrame,
                     output_column_names: dict[str, str],
                     settings: dict) -> pd.DataFrame:

        df[output_column_names["support"]] = 0.

        df['_support_zone_upper'] = df[settings['support_signal']] * (1 + settings['factor'])
        df['_support_zone_lower'] = df[settings['support_signal']] * (1 - settings['factor'])
        if settings['direction'] == SupportIndicator.SUPPORT_DIRECTION_UP:
            df.loc[((df[settings['signal']].shift(2) > df['_support_zone_upper'])
                    & (df[settings['signal']].shift() <= df['_support_zone_upper'])
                    & (df[settings['signal']].shift() >= df['_support_zone_lower'])
                    & (df[settings['signal']] > df['_support_zone_upper'])), output_column_names['support']] = 1.0
        elif settings['direction'] == SupportIndicator.SUPPORT_DIRECTION_DOWN:
            df.loc[((df[settings['signal']].shift(2) < df['_support_zone_lower'])
                    & (df[settings['signal']].shift() <= df['_support_zone_upper'])
                    & (df[settings['signal']].shift() >= df['_support_zone_lower'])
                    & (df[settings['signal']] < df['_support_zone_lower'])), output_column_names['support']] = 1.0
        else:
            raise ValueError(f"Did not understand support direction {settings['direction']}")
        return df


class BreakoutDetector(Indicator):

    BREAKOUT_DIRECTION_UP = "up"
    BREAKOUT_DIRECTION_DOWN = "down"

    def __init__(self,
                 direction: str,
                 threshold_signal: str,
                 *args,
                 signal: str = "close",
                 **kwargs):
        self.direction = direction
        self.threshold_signal = threshold_signal
        self.signal = signal
        kwargs['setting_attrs'] = ["direction", "signal", "threshold_signal"]
        super().__init__(*args, **kwargs)

    def get_default_column_names_impl(self,
                                      output_column_names: dict[str, str],
                                      settings: dict) -> dict[str, str]:

        output_column_names.update({"breakout": f"breakout_{settings['direction']}_of_{settings['threshold_signal']}_by_{settings['signal']}"})
        return output_column_names

    def compute_impl(self, df: pd.DataFrame,
                     output_column_names: dict[str, str],
                     settings: dict | None = None) -> pd.DataFrame:

        df[output_column_names["breakout"]] = 0.
        if settings['direction'] == BreakoutDetector.BREAKOUT_DIRECTION_UP:
            df.loc[df[settings['signal']].shift() >= df[settings['threshold_signal']], output_column_names["breakout"]] = 1.0
        elif settings['direction'] == BreakoutDetector.BREAKOUT_DIRECTION_DOWN:
            df.loc[df[settings['signal']].shift() <= df[settings['threshold_signal']], output_column_names["breakout"]] = -1.0
        else:
            raise ValueError(f"Direction {settings['direction']} not understood.")
        
        return df



class PostBreakoutCrossDetector(Indicator):

    def __init__(self,
                 condition_signals: str,
                 negation_signal: str,
                 *args,
                 **kwargs):
        self.condition_signals = condition_signals
        self.negation_signal = negation_signal
        kwargs['setting_attrs'] = ["condition_signals", "negation_signal"]
        super().__init__(*args, **kwargs)

    def get_default_column_names_impl(self,
                                      output_column_names: dict[str, str],
                                      settings: dict) -> dict[str, str]:

        output_column_names.update({"signal": f"condition_hold_{'_'.join(settings['condition_signals'])}_{settings['negation_signal']}"})
        return output_column_names

    def compute_impl(self, df: pd.DataFrame,
                     output_column_names: dict[str, str],
                     settings: dict | None = None) -> pd.DataFrame:
        
        df["_sig"] = df[settings['condition_signals'][0]]
        for ii in range(len(settings['condition_signals'])):
            df["_sig"] = df["_sig"] + df[settings['condition_signals'][ii]]
        df.loc[df["_sig"] == 0., "_sig"] = pd.NA
        df["_sig"].ffill(inplace=True)
        negation_up = (df["close"] > df[settings["negation_signal"]]) & (df["open"] < df[settings["negation_signal"]]) & (df["_sig"] < 0)
        negation_down = (df["close"] < df[settings["negation_signal"]]) & (df["open"] > df[settings["negation_signal"]]) & (df["_sig"] > 0)
        df[output_column_names["signal"]] = 0.
        df.loc[negation_up, output_column_names["signal"]] = 1.0
        df.loc[negation_down, output_column_names["signal"]] = -1.0
        return df




class CCIIndicator(Indicator):

    def __init__(self,
                 *args,
                 period: int = 14,
                 **kwargs):
        self.period = period
        kwargs['setting_attrs'] = ["period",]
        super().__init__(*args, **kwargs)

    def get_default_column_names_impl(self,
                                      output_column_names: dict[str, str],
                                      settings: dict) -> dict[str, str]:

        output_column_names.update({"cci": f"cci_{settings['period']}"})
        return output_column_names

    def compute_impl(self, df: pd.DataFrame,
                     output_column_names: dict[str, str],
                     settings: dict | None = None) -> pd.DataFrame:

        df[output_column_names["cci"]] = talib.CCI(high=df["high"],
                                                   low=df["low"],
                                                   close=df["close"],
                                                   timeperiod=settings['period'])

        return df


class StochRSIIndicator(Indicator):

    def __init__(self,
                 *args,
                 signal: str = "close",
                 period: int = 14,
                 fastk_period: int = 5,
                 fastd_period: int = 3,
                 fastd_matype: int = 0,
                 **kwargs):
        self.signal = signal
        self.period = period
        self.fastk_period = fastk_period
        self.fastd_period = fastd_period
        self.fastd_matype = fastd_matype
        kwargs['setting_attrs'] = ["period",
                                   "signal",
                                   "fastk_period",
                                   "fastd_period",
                                   "fastd_matype"]
        super().__init__(*args, **kwargs)

    def get_default_column_names_impl(self,
                                      output_column_names: dict[str, str],
                                      settings: dict) -> dict[str, str]:

        output_column_names.update({"fastk": f"stochrsi_fastk_{settings['signal']}_{settings['period']}",
                                    "fastd": f"stochrsi_fastd_{settings['signal']}_{settings['period']}"})
        return output_column_names

    def compute_impl(self, df: pd.DataFrame,
                     output_column_names: dict[str, str],
                     settings: dict | None = None) -> pd.DataFrame:

        df[output_column_names["fastk"]], df[output_column_names["fastd"]] = talib.STOCHRSI(df[settings['signal']],
                                                                                            timeperiod=settings['period'],
                                                                                            fastk_period=settings["fastk_period"],
                                                                                            fastd_period=settings["fastd_period"],
                                                                                            fastd_matype=settings["fastd_matype"])

        return df


class WilliamsFractals(Indicator):

    def __init__(self, period: int = 2, *args, **kwargs):
        self.period = period
        kwargs["setting_attrs"] = ["period"]
    
    
    def get_default_column_names_impl(self,
                                      output_column_names: dict[str, str],
                                      settings: dict) -> dict[str, str]:

        output_column_names.update({"up_fractal": "up_fractal",
                                    "down_fractal": "down_fractal"})
        return output_column_names

    def compute_impl(self, df: pd.DataFrame, output_column_names: dict[str, str], settings: dict) -> pd.DataFrame:
        period = settings["period"]
        window = 2 * period + 1 # default 5

        bears = df['high'].rolling(window, center=True).apply(lambda x: x[period] == max(x), raw=True)
        bulls = df['low'].rolling(window, center=True).apply(lambda x: x[period] == min(x), raw=True)
        df[output_column_names["up_fractal"]] = bulls
        df[output_column_names["down_fractal"]] = bears

        return df



class CCDStochRSIScalpSignalGenerator(Indicator):

    def __init__(self,
                 *args,
                 ema_signal1: str = "EMA_close_9",
                 ema_signal2: str = "EMA_close_20",
                 ema_signal3: str = "EMA_close_50",
                 cci_signal: str = "cci_14",
                 stochrsi_fastk: str = "stochrsi_fastk_close_14",
                 stochrsi_fastd: str = "stochrsi_fastd_close_14",
                 rsi_oversold_value: int = 30,
                 rsi_overbought_value: int = 70,
                 cci_trigger: int = 100,
                 **kwargs):
        self.ema_signal1 = ema_signal1
        self.ema_signal2 = ema_signal2
        self.ema_signal3 = ema_signal3
        self.cci_signal = cci_signal
        self.rsi_oversold_value = rsi_oversold_value
        self.rsi_overbought_value = rsi_overbought_value
        self.cci_trigger = cci_trigger
        self.stochrsi_fastk = stochrsi_fastk
        self.stochrsi_fastd = stochrsi_fastd
        kwargs['setting_attrs'] = ["ema_signal1",
                                   "ema_signal2",
                                   "ema_signal3",
                                   "cci_signal",
                                   "stochrsi_fastk",
                                   "stochrsi_fastd",
                                   "rsi_oversold_value",
                                   "rsi_overbought_value",
                                   "cci_trigger"]
        super().__init__(*args, **kwargs)

    def get_default_column_names_impl(self,
                                      output_column_names: dict[str, str],
                                      settings: dict) -> dict[str, str]:

        output_column_names.update({"scalpsignal": f"ccdstochrsi_scalp_signal_"
                                                   f"{settings['ema_signal1']}"
                                                   f"_{settings['ema_signal2']}"
                                                   f"_{settings['ema_signal3']}"
                                                   f"_{settings['cci_signal']}"
                                                   f"_{settings['stochrsi_fastk']}"})
        return output_column_names

    def compute_impl(self, df: pd.DataFrame,
                     output_column_names: dict[str, str],
                     settings: dict | None = None) -> pd.DataFrame:

        df[output_column_names["scalpsignal"]] = 0
        long_condition = df[settings['ema_signal3']] < df['low']
        long_condition = long_condition & (df[settings['stochrsi_fastk']] <= settings['rsi_oversold_value'])
        #long_condition = long_condition & (df[settings['stochrsi_fastk']] >= df[settings['stochrsi_fastd']])
        long_condition = long_condition & (df[settings['cci_signal']] <= -settings['cci_trigger'])
        df.loc[long_condition, output_column_names['scalpsignal']] = 1.0
        short_condition = df[settings['ema_signal3']] > df['high']
        short_condition = short_condition & (df[settings['stochrsi_fastk']] >= settings['rsi_overbought_value'])
        #short_condition = short_condition & (df[settings['stochrsi_fastk']] <= df[settings['stochrsi_fastd']])
        short_condition = short_condition & (df[settings['cci_signal']] >= settings['cci_trigger'])
        df.loc[short_condition, output_column_names['scalpsignal']] = -1.0
        return df


