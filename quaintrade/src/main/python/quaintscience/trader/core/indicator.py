from abc import ABC, abstractmethod
from typing import Optional, Union
import copy

import pandas as pd
import pandas_ta as pd_ta
import talib

from .logging import LoggerMixin


class Indicator(ABC, LoggerMixin):

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

    def preprocess(self, df: pd.DataFrame,
                output_column_name: Optional[Union[str, list[str]]] = None,
                settings: Optional[dict] = None) -> (pd.DataFrame, Optional[Union[str, list[str]]], Optional[dict]):
        if settings is None:
            settings = {}
        for attr in self.setting_attrs:
            self.add_setting(settings, attr)
        if output_column_name is None:
            output_column_name = {}
        return df, output_column_name, settings

    def postprocess(self, df: pd.DataFrame,
                output_column_name: Optional[Union[str, dict[str, str]]] = None,
                settings: Optional[dict] = None) -> (pd.DataFrame, Optional[Union[str, list[str]]], Optional[dict]):
        for _, column_name in output_column_name.items():
            df[column_name] = df[column_name].astype(float)
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


class SlopeIndicator(Indicator):
    def __init__(self, signal: str, *args, shift: int = 1,
                 **kwargs):
        self.signal = signal
        self.shift = shift
        kwargs["setting_attrs"] = ["signal", "shift"]
        super().__init__(*args, **kwargs)
    
    def compute_impl(self, df: pd.DataFrame,
                     output_column_name: str | dict[str, str] | None = None,
                     settings: dict | None = None) -> pd.DataFrame:
        if output_column_name is None or len(output_column_name) == 0:
            output_column_name["slope"] = f"{settings['signal']}_slope"
            output_column_name["acceleration"] = f"{settings['signal']}_acceleration"
        df[output_column_name["slope"]] = df[settings['signal']].diff(periods=settings['shift'])
        df[output_column_name["acceleration"]] = df[settings['signal']].diff(settings['shift']).diff()
        return df, output_column_name, settings        


class DonchainIndicator(Indicator):

    def __init__(self, *args, period: int = 15, **kwargs):
        self.period = period
        kwargs["setting_attrs"] = ["period"]
        super().__init__(*args, **kwargs)

    def compute_impl(self, df: pd.DataFrame,
                output_column_name: Optional[Union[str, dict[str, str]]] = None,
                settings: Optional[dict] = None) -> pd.DataFrame:
        if output_column_name is None or len(output_column_name) == 0:
            x = ["donchainUpper", "donchainMiddle", "donchainLower"]
            y = [f"{val}_{settings['period']}" for val in x]
            output_column_name = dict(zip(x, y))
        df[output_column_name["donchainUpper"]] = df["high"].rolling(settings['period']).apply(lambda x: max(x))
        df[output_column_name["donchainLower"]] = df["low"].rolling(settings['period']).apply(lambda x: min(x))
        df[output_column_name["donchainMiddle"]] = (df[output_column_name["donchainUpper"]] + df[output_column_name["donchainLower"]]) /2
        return df, output_column_name, settings


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

    def compute_impl(self, df: pd.DataFrame,
                     output_column_name: str | dict[str, str] | None = None,
                     settings: dict | None = None) -> pd.DataFrame:

        if output_column_name is None or len(output_column_name) == 0:
            x = ["pullback_start", "pullback_end"]
            y = [f"{settings['breakout_column']}_{val}" for val in x]
            output_column_name = dict(zip(x, y))
        after_breakout = False
        pull_back_in_progress = False

        df["_breakouts"] = 0.0
        if settings["pullback_direction"] == PullbackDetector.PULLBACK_DIRECTION_DOWN:
            df.loc[df[settings["price_column"]] >= df[settings["breakout_column"]], "_breakouts"] = 1.0
        else:
            df.loc[df[settings["price_column"]] <= df[settings["breakout_column"]], "_breakouts"] = 1.0

        prev_row = None
        for v in output_column_name.values():
            df[v] = 0.
        for _, row in df.iterrows():
            if settings["pullback_direction"] == PullbackDetector.PULLBACK_DIRECTION_DOWN:
                if (prev_row is not None
                    and row["_breakouts"] != 1.0
                    and prev_row["_breakouts"] == 1.0):
                        df.loc[row.name, output_column_name["pullback_start"]] = 1.0
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
                        df.loc[row.name, output_column_name["pullback_end"]] = 1.0
                        pull_back_in_progress = False
                        after_breakout = False
                prev_row = row
            if settings["pullback_direction"] == PullbackDetector.PULLBACK_DIRECTION_UP:
                if (prev_row is not None
                    and row["_breakouts"] != 1.0
                    and prev_row["_breakouts"] == 1.0):
                        df.loc[row.name, output_column_name["pullback_start"]] = 1.0
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
                        df.loc[row.name, output_column_name["pullback_end"]] = 1.0
                        pull_back_in_progress = False
                        after_breakout = False
                prev_row = row

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
        kwargs["setting_attrs"] = ["period_interval", "data_interval", "shift"]
        super().__init__(*args, **kwargs)

    def compute_impl(self, df: pd.DataFrame,
                output_column_name: Optional[Union[str, dict[str, str]]] = None,
                settings: Optional[dict] = None) -> pd.DataFrame:

        if output_column_name is None or len(output_column_name) == 0:
            x = ["previous_high", "previous_low"]
            y = [f"{val}_{settings['period_interval']}_{settings['shift']}" for val in x]
            output_column_name = dict(zip(x, y))

        pwh = df["high"].resample(settings["period_interval"]).apply("max").shift(settings["shift"],
                                                                                  freq=settings["period_interval"])
        df[output_column_name["previous_high"]] = pwh.resample(settings["data_interval"]).ffill().ffill()
        pwl = df["low"].resample(settings["period_interval"]).apply("min").shift(settings["shift"],
                                                                                 freq=settings["period_interval"])
        df[output_column_name["previous_low"]] = pwl.resample(settings["data_interval"]).ffill().ffill()
        return df, output_column_name, settings


class SMAIndicator(Indicator):
        
    def __init__(self, *args,
                 period: int = 22,
                 signal: str = "close",
                 **kwargs):
        self.period = period
        self.signal = signal
        kwargs["setting_attrs"] = ["period", "signal"]
        super().__init__(*args, **kwargs)

    def compute_impl(self, df: pd.DataFrame,
                output_column_name: Optional[Union[str, dict[str, str]]] = None,
                settings: Optional[dict] = None) -> pd.DataFrame:
        if output_column_name is None or len(output_column_name) == 0:
            output_column_name = {"SMA": f"SMA_{settings['period']}"}
            if settings['signal'] != 'close':
                output_column_name = {"SMA": f"{output_column_name['SMA']}_{settings['signal']}"}
        df[output_column_name["SMA"]] = talib.SMA(df[settings['signal']], timeperiod=self.period)
        return df, output_column_name, settings


class WMAIndicator(Indicator):
        
    def __init__(self, *args,
                 period: int = 22,
                 **kwargs):
        self.period = period
        kwargs["setting_attrs"] = ["period"]
        super().__init__(*args, **kwargs)

    def compute_impl(self, df: pd.DataFrame,
                output_column_name: Optional[Union[str, dict[str, str]]] = None,
                settings: Optional[dict] = None) -> pd.DataFrame:
        if output_column_name is None or len(output_column_name) == 0:
            output_column_name = {"WMA": f"WMA_{self.period}"}
        df[output_column_name["WMA"]] = talib.SMA(df[settings.get("input_column", "close")], timeperiod=self.period)
        return df, output_column_name, settings


class ADXIndicator(Indicator):

    def __init__(self, *args,
                 period: int = 14,
                 **kwargs):
        self.period = period
        kwargs["setting_attrs"] = ["period"]
        super().__init__(*args, **kwargs)

    def compute_impl(self, df: pd.DataFrame,
                output_column_name: Optional[Union[str, dict[str, str]]] = None,
                settings: Optional[dict] = None) -> pd.DataFrame:
        if output_column_name is None or len(output_column_name) == 0:
            output_column_name = {"ADX": f"ADX_{settings['period']}"}
        df[output_column_name["ADX"]] = talib.ADX(df["high"], df["low"], df["close"], timeperiod=settings['period'])
        return df, output_column_name, settings
    

class ATRIndicator(Indicator):

    def __init__(self, *args,
                 period: int = 14,
                 **kwargs):
        self.period = period
        kwargs["setting_attrs"] = ["period"]
        super().__init__(*args, **kwargs)

    def compute_impl(self, df: pd.DataFrame,
                output_column_name: Optional[Union[str, dict[str, str]]] = None,
                settings: Optional[dict] = None) -> pd.DataFrame:
        if output_column_name is None or len(output_column_name) == 0:
            output_column_name = {"ATR": f"ATR_{settings['period']}"}
        df[output_column_name["ATR"]] = talib.ADX(df["high"], df["low"], df["close"], timeperiod=settings['period'])
        return df, output_column_name, settings


class RSIIndicator(Indicator):
        
    def __init__(self,
                 *args,
                 period: int = 14,
                 **kwargs):
        self.period = period
        kwargs["setting_attrs"] = ["period"]
        super().__init__(*args, **kwargs)

    def compute_impl(self, df: pd.DataFrame,
                output_column_name: Optional[Union[str, dict[str, str]]] = None,
                settings: Optional[dict] = None) -> pd.DataFrame:
        if output_column_name is None or len(output_column_name) == 0:
            output_column_name = {"RSI": f"RSI_{settings['period']}"}
        
        df[output_column_name["RSI"]] = talib.RSI(df["close"], timeperiod=settings['period'])
        return df, output_column_name, settings


class ChoppinessIndicator(Indicator):
        
    def __init__(self,
                 *args,
                 period: int = 14,
                 **kwargs):
        self.period = period
        kwargs["setting_attrs"] = ["period"]
        super().__init__(*args, **kwargs)

    def compute_impl(self, df: pd.DataFrame,
                output_column_name: Optional[Union[str, dict[str, str]]] = None,
                settings: Optional[dict] = None) -> pd.DataFrame:
        if output_column_name is None or len(output_column_name) == 0:
            output_column_name = {"choppiness": f"choppiness_{settings['period']}"}
        print(pd_ta.choppiness(df["high"], df["low"], df["close"], length=settings['period'], multiplier=settings['multiplier']))
        df[output_column_name["supertrend"]] = pd_ta.supertrend(high=df["high"], low=df["low"], close=df["close"],
                                                                length=settings['period'],
                                                                multiplier=settings['multiplier'])[f"SUPERT_{settings['period']}_{settings['multiplier']:.1f}"]
        df[output_column_name["supertrend"]].fillna(df["close"].mean(), inplace=True)
        df.loc[df[output_column_name["supertrend"]] < 1e-3, output_column_name["supertrend"]] = df["close"].mean()
        return df, output_column_name, settings
    

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

    def compute_impl(self, df: pd.DataFrame,
                output_column_name: Optional[Union[str, dict[str, str]]] = None,
                settings: Optional[dict] = None) -> pd.DataFrame:
        if output_column_name is None or len(output_column_name) == 0:
            output_column_name = {"supertrend": f"supertrend_{settings['period']}_{settings['multiplier']:.1f}"}
        df[output_column_name["supertrend"]] = pd_ta.supertrend(high=df["high"], low=df["low"], close=df["close"],
                                                                length=settings['period'],
                                                                multiplier=settings['multiplier'])[f"SUPERT_{settings['period']}_{settings['multiplier']:.1f}"]
        df[output_column_name["supertrend"]].fillna(df["close"].mean(), inplace=True)
        df.loc[df[output_column_name["supertrend"]] < 1e-3, output_column_name["supertrend"]] = df["close"].mean()
        return df, output_column_name, settings
    


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

    def compute_impl(self, df: pd.DataFrame,
                     output_column_name: Optional[Union[str, dict[str, str]]] = None,
                     settings: Optional[dict] = None) -> pd.DataFrame:
        if output_column_name is None or len(output_column_name) == 0:
            x = ["BBandUpper", "BBandMiddle", "BBandLower"]
            y = [f"{val}_{settings['period']}" for val in x]
            output_column_name = dict(zip(x, y))
        
        df[output_column_name["BBandUpper"]], df[output_column_name["BBandMiddle"]], df[output_column_name["BBandLower"]] = talib.BBANDS(df["close"],
                                                                                                                                         timeperiod=settings['period'],
                                                                                                                                         nbdevup=settings['nbdevup'],
                                                                                                                                         nbdevdn=settings['nbdevdown'])
        return df, output_column_name, settings


class CDLPatternIndicator(Indicator):
        
    def __init__(self, *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

    def compute_impl(self, df: pd.DataFrame,
                     output_column_name: Optional[Union[str, dict[str, str]]] = None,
                     settings: Optional[dict] = None) -> pd.DataFrame:
        if output_column_name is None or len(output_column_name) == 0:
            for item in dir(talib):
                if item.startswith("CDL"):
                    output_column_name[item] = item

        for item in output_column_name.keys():
            df[item] = getattr(talib, item)(df["open"], df["high"], df["low"], df["close"])
        return df, output_column_name, settings


class BreakoutIndicator(Indicator):
        
    def __init__(self,
                 upper_breakout_column: str,
                 lower_breakout_column: str,
                 *args,
                 upper_price_column: str = "high",
                 lower_price_column: str = "low",
                 **kwargs):
        self.upper_breakout_column = upper_breakout_column
        self.lower_breakout_column = lower_breakout_column
        self.upper_price_column = upper_price_column
        self.lower_price_column = lower_price_column
        kwargs['setting_attrs'] = ["upper_breakout_column", "lower_breakout_column",
                                   "upper_price_column", "lower_price_column"]
        super().__init__(*args, **kwargs)

    def compute_impl(self, df: pd.DataFrame,
                     output_column_name: Optional[Union[str, dict[str, str]]] = None,
                     settings: Optional[dict] = None) -> pd.DataFrame:

        if output_column_name is None or len(output_column_name) == 0:
            output_column_name = {"upperBreakout": f"{settings['upper_breakout_column']}_breakout",
                                  "lowerBreakout": f"{settings['lower_breakout_column']}_breakout"}
        df[output_column_name["upperBreakout"]] = 0.
        df[output_column_name["lowerBreakout"]] = 0.
        df.loc[((df[settings['upper_price_column']] > df[settings['upper_breakout_column']].shift()) &
                (df[settings['upper_price_column']].shift() <= df[settings['upper_breakout_column']].shift(2)) &
                (df["close"] >= df["open"])),
                output_column_name["upperBreakout"]] = 1.0

        df.loc[((df[settings['lower_price_column']] < df[settings['lower_breakout_column']].shift()) &
                (df[settings['lower_price_column']].shift() >= df[settings['lower_breakout_column']].shift(2)) &
                (df["close"] <= df["open"])),
                output_column_name["lowerBreakout"]] = 1.0
        return df, output_column_name, settings


class HeikinAshiIndicator(Indicator):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def smooth(self, df, field):
        prev_row = None
        prev_prev_row = None
        prev_row_id = None
        for row_id, row in df.iterrows():
            if prev_row is None:
                prev_row = row
                prev_row_id = row_id
                continue
            if prev_prev_row is None:
                prev_prev_row = prev_row
                continue
            if prev_prev_row[field] == 1. and prev_row[field] == 0. and row[field] == 1.:
                df.loc[prev_row_id, field] = 1.0
            prev_prev_row = prev_row
            prev_row = row
            prev_row_id = row_id


    def compute_impl(self, df: pd.DataFrame,
                     output_column_name: str | dict[str, str] | None = None,
                     settings: dict | None = None) -> pd.DataFrame:

        x = ["high", "low", "open", "close"]
        output_column_name = dict(zip(x, x))

        heikin_ashi_df = pd.DataFrame(index=df.index.values,
                                      columns=['open', 'high', 'low', 'close'])
    
        heikin_ashi_df['close'] = (df['open'] + df['high'] + df['low'] + df['close']) / 4
        
        for i in range(len(df)):
            if i == 0:
                heikin_ashi_df.iat[0, 0] = df['open'].iloc[0]
            else:
                heikin_ashi_df.iat[i, 0] = (heikin_ashi_df.iat[i-1, 0] + heikin_ashi_df.iat[i-1, 3]) / 2
            
        heikin_ashi_df['high'] = heikin_ashi_df.loc[:, ['open', 'close']].join(df['high']).max(axis=1)
        
        heikin_ashi_df['low'] = heikin_ashi_df.loc[:, ['open', 'close']].join(df['low']).min(axis=1)
        
        for col in ["open", "high", "low", "close"]:
            df[col] = heikin_ashi_df[col].astype(float)

        df["ha_trending_green"] = 0.
        df["ha_trending_red"] = 0.
        df["ha_non_trending"] = 0.
        df["ha_long_trend"] = 0.
        df["ha_short_trend"] = 0.

        for row_id, row in df.iterrows():
            if self.green_trending_candle(row):
                df.loc[row_id, "ha_trending_green"] = 1.
            elif self.red_trending_candle(row):
                df.loc[row_id, "ha_trending_red"] = 1.
            if self.doji_candle(row):
                df.loc[row_id, "ha_non_trending"] = 1.

        #self.smooth(df, "ha_trending_green")
        #self.smooth(df, "ha_trending_green")
        #self.smooth(df, "ha_trending_red")
        #self.smooth(df, "ha_trending_red")
        #self.smooth(df, "ha_non_trending")
        #self.smooth(df, "ha_non_trending")

        prev_row = None
        short_trend_in_progress = False
        long_trend_in_progress = False
        for row_id, row in df.iterrows():
            if prev_row is None:
                prev_row = row
                continue
            if ((prev_row["ha_trending_green"] == 1.0
                or prev_row["ha_non_trending"] == 1.0)
                and row["ha_trending_red"] == 1.0):
                short_trend_in_progress = True
                long_trend_in_progress = False
            elif ((prev_row["ha_trending_red"] == 1.0
                or prev_row["ha_non_trending"] == 1.0)
                and row["ha_trending_green"] == 1.0):
                short_trend_in_progress = False
                long_trend_in_progress = True
            elif row["ha_non_trending"] == 1.0:
                short_trend_in_progress = False
                long_trend_in_progress = False

            if short_trend_in_progress:
                df.loc[row_id, "ha_short_trend"] = 1.0
            elif long_trend_in_progress:
                df.loc[row_id, "ha_long_trend"] = 1.0
            prev_row = row
        return df, output_column_name, settings

    def green_trending_candle(self, row):
        if (row["close"] > row["open"]):
            #and ((row["low"] - row["open"]) < 0.1 * (row["close"] - row["open"]))):
            return True
        return False

    def red_trending_candle(self, row):
        if (row["close"] < row["open"]):
            #and ((row["high"] - row["open"]) < 0.1 * (row["open"] - row["close"]))):
            return True
        return False

    def doji_candle(self, row):
        if ((row["high"] - max(row["open"], row["close"])) > 0.2 * abs(row["open"] - row["close"])
            and ((min(row["open"], row["close"]) - row["low"]) > 0.2 * abs(row["close"] - row["open"]))):
            return True
        return False


class SupportIndicator(Indicator):

    SUPPORT_DIRECTION_UP = "up"
    SUPPORT_DIRECTION_DOWN = "down"

    def __init__(self,
                 direction: str,
                 signal: str,
                 *args,
                 factor: float = 0.04/100,
                 max_candle_size: float = 20.,
                 **kwargs):
        self.direction = direction
        self.signal = signal
        self.factor = factor
        self.max_candle_size = max_candle_size
        kwargs['setting_attrs'] = ["direction", "signal", "factor", "max_candle_size"]
        super().__init__(*args, **kwargs)

    def compute_impl(self, df: pd.DataFrame, output_column_name: str | dict[str, str] | None = None,
                     settings: dict | None = None) -> pd.DataFrame:

        if output_column_name is None or len(output_column_name) == 0:
            output_column_name = {"support": f"{settings['signal']}_{settings['direction']}_support"}

        df[output_column_name["support"]] = 0.

        df['_support_zone_upper'] = df[settings['signal']] * (1 + settings['factor'])
        df['_support_zone_lower'] = df[settings['signal']] * (1 - settings['factor'])

        df["_started_search"] = 0.
        awaiting_approach = False

        for _, row in df.iterrows():
            # print(row["low"], row["_support_zone_upper"])
            if settings['direction'] == SupportIndicator.SUPPORT_DIRECTION_UP:
                if (not awaiting_approach
                    and row["low"] >= row["_support_zone_upper"]):
                    df["_started_search"] = 1.
                    awaiting_approach = True
                    continue
                elif awaiting_approach:
                    if row["high"] <= row["_support_zone_lower"] or abs(row["close"] - row["open"]) > settings["max_candle_size"]:
                        awaiting_approach = False
                        continue
                    elif ((row["_support_zone_upper"] >= row["close"] >= row["_support_zone_lower"]
                           or row["_support_zone_upper"] >= row["open"] >= row["_support_zone_lower"])
                           #or (row["_support_zone_upper"] < row["close"] and row["_support_zone_lower"] > row["open"]))
                          and self.is_green_candle(row)):
                         df.loc[row.name, output_column_name["support"]] = 1.0
                         awaiting_approach = False
            elif settings["direction"] == SupportIndicator.SUPPORT_DIRECTION_DOWN:
                if (not awaiting_approach
                    and row["high"] <= row["_support_zone_lower"]):
                    awaiting_approach = True
                    continue
                if awaiting_approach:
                    if row["low"] >= row["_support_zone_upper"] or abs(row["close"] - row["open"]) > settings["max_candle_size"]:
                        awaiting_approach = False
                        continue
                    elif ((row["_support_zone_upper"] >= row["close"] >= row["_support_zone_lower"]
                           or row["_support_zone_upper"] >= row["open"] >= row["_support_zone_lower"])
                           #or (row["_support_zone_upper"] < row["open"] and row["_support_zone_lower"] > row["close"]))
                          and self.is_red_candle(row)):
                         df.loc[row.name, output_column_name["support"]] = 1.0
                         awaiting_approach = False
        return df, output_column_name, settings


