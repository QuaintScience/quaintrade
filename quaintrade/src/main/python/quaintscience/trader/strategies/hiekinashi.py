from typing import Optional
#import datetime

import pandas as pd

from ..core.ds import (TradeType, OrderState)

from ..core.strategy import (StrategyExecutor)
from ..core.indicator import (IndicatorPipeline,
                              HeikinAshiIndicator,
                              SupertrendIndicator,
                              SMAIndicator,
                              SlopeIndicator,
                              DonchainIndicator)
from ..core.util import resample_candle_data



class HiekinAshiStrategy(StrategyExecutor):

    def __init__(self,
                 *args,
                 st_period: int = 7,
                 st_multiplier: float = 2.5,
                 ma_period: int = 10,
                 donchain_period: int = 15,
                 **kwargs):
        self.st_period = st_period
        self.st_multiplier = st_multiplier
        self.ma_period = ma_period
        self.donchain_period = donchain_period
        indicators = indicators=[(HeikinAshiIndicator(), None, None),
                                 (SupertrendIndicator(period=self.st_period,
                                                      multiplier=self.st_multiplier), None, None),
                                 (SMAIndicator(period=self.ma_period), None, None),
                                 (SlopeIndicator(signal=f"SMA_{self.ma_period}"), None, None),
                                 (DonchainIndicator(period=self.donchain_period), None, None)]
        kwargs["indicator_pipeline"] = IndicatorPipeline(indicators=indicators)
        kwargs["indicator_fields"] = [{"field": "ha_long_trend", "panel": 2},
                                      {"field": "ha_short_trend", "panel": 3},
                                      {"field": "ha_non_trending", "panel": 4},
                                      {"field": f"SMA_{self.ma_period}_slope", "panel": 5},
                                      f"supertrend_{self.st_period}_{self.st_multiplier:.1f}",
                                      f"SMA_{self.ma_period}",
                                      f"donchainUpper_{self.donchain_period}",
                                      f"donchainLower_{self.donchain_period}",]
        non_trading_timeslots = []
        non_trading_timeslots.extend(StrategyExecutor.NON_TRADING_FIRST_HOUR)
        non_trading_timeslots.extend(StrategyExecutor.NON_TRADING_AFTERNOON)
        kwargs["non_trading_timeslots"] = non_trading_timeslots
        self.current_run = None
        kwargs["plot_results"] = False
        self.target_amt = 400
        #self.entry_threshold = 2
        self.entry_threshold = 0.1
        print(args, kwargs)
        super().__init__(*args, **kwargs)

    def get_entry(self, window: pd.DataFrame, trade_type: TradeType):
        if trade_type == TradeType.LONG:
            return window.iloc[-1]["high"] + self.entry_threshold
        else:
            return window.iloc[-1]["low"] - self.entry_threshold

    def get_target(self, window: pd.DataFrame, trade_type: TradeType):
        pass

    def get_stoploss(self, window: pd.DataFrame, trade_type: TradeType):
        pass

    def update_stoplosses(self, window):
        for order in self.trade_manager.get_orders():
            if (order.state == OrderState.PENDING
                and "hiekinashi_long" in order.tags
                and "sl_order" in order.tags):
               order.limit_price = window.iloc[-1]["low"] - self.stoploss_threshold
               self.trigger_price = window.iloc[-1]["low"] - self.stoploss_threshold
               self.trade_manager.update_order(order)
            if (order.state == OrderState.PENDING
                and "hiekinashi_short" in order.tags
                and "sl_order" in order.tags):
               order.limit_price = window.iloc[-1]["high"] + self.stoploss_threshold
               self.trigger_price = window.iloc[-1]["high"] + self.stoploss_threshold
               self.trade_manager.update_order(order)

    def cancel_active_orders(self):
        for order in self.trade_manager.get_orders():
            if (order.state == OrderState.PENDING
                and ("hiekinashi_long" in order.tags
                     or "hiekinashi_short" in order.tags)):
                self.trade_manager.cancel_order(order)
        self.perform_squareoff()

    def strategy(self, window: pd.DataFrame,
                 context: dict[str, pd.DataFrame]) -> Optional[TradeType]:
        #weekly_context = resample_candle_data(context_df, "1w")
        #weekly_context = self.indicator_pipeline.compute(weekly_context)
        colvals = []
        for col in window.columns:
            if col not in ["open", "high", "low", "close"]:
                colvals.append(col)
                colvals.append(str(window.iloc[-1][col]))
        self.logger.info(f"Strategy: timestamp {window.iloc[-1].index.name}; OHLC"
                         f" {window.iloc[-1]['open']}"
                         f" {window.iloc[-1]['high']}"
                         f" {window.iloc[-1]['low']}"
                         f" {window.iloc[-1]['close']}"
                         f"{' '.join(colvals)}")

        if window.iloc[-1]["ha_non_trending"] == 1.0:
            self.cancel_active_orders()
        if self.can_trade(window):
            #if (window.iloc[-1]["ha_non_trending"] == 1.0 and window.iloc[-2]["ha_non_trending"] == 1.0):
            #    return
            if (window.iloc[-1]["ha_long_trend"] == 1.0 and window.iloc[-2]["ha_long_trend"] != 1.0
                and context["1d"].iloc[-1]["ha_long_trend"] == 1.0):
                #and window.iloc[-1][f"supertrend_{self.st_period}_{self.st_multiplier:.1f}"] < window.iloc[-1].close):
                self.cancel_active_orders()
                self.take_position(window,
                                trade_type=TradeType.LONG, tags=[f"hiekinashi_long"])
                self.current_run = TradeType.LONG
            if (window.iloc[-1]["ha_short_trend"] == 1.0 and window.iloc[-2]["ha_short_trend"] != 1.0
                and context["1d"].iloc[-1]["ha_short_trend"] == 1.0):
                #and window.iloc[-1][f"supertrend_{self.st_period}_{self.st_multiplier:.1f}"] > window.iloc[-1].open):
                self.cancel_active_orders()
                self.current_run = TradeType.SHORT
                self.take_position(window,
                                   trade_type=TradeType.SHORT, tags=[f"hiekinashi_short"])

        return None
