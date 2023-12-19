from typing import Optional

import pandas as pd
import numpy as np

from ..core.ds import (TradeType, OrderType,
                       OrderState, TransactionType)
from ..core.util import new_id
from ..core.strategy import (StrategyExecutor)
from ..core.indicator import (IndicatorPipeline,
                              HeikinAshiIndicator,
                              DonchainIndicator,
                              PullbackDetector)



class DonchainPullbackStrategy(StrategyExecutor):

    def __init__(self,
                 *args,
                 donchain_period: int = 15,
                 **kwargs):
        indicators = indicators=[(HeikinAshiIndicator(), None, None),
                                 (DonchainIndicator(period=donchain_period), None, None),
                                 (PullbackDetector(breakout_column=f"donchainUpper_{donchain_period}",
                                                                     price_column="high",
                                                                     pullback_direction=PullbackDetector.PULLBACK_DIRECTION_DOWN),
                                                     None, None),
                                 (PullbackDetector(breakout_column=f"donchainLower_{donchain_period}",
                                                                     price_column="low",
                                                                     pullback_direction=PullbackDetector.PULLBACK_DIRECTION_UP),
                                                     None, None)]
        kwargs["indicator_pipeline"] = IndicatorPipeline(indicators=indicators)
        kwargs["long_entry_price_column"] = "high"
        kwargs["short_entry_price_column"] = "low"
        kwargs["sl_price_column_long"] = "high"
        kwargs["sl_price_column_short"] = "low"
        kwargs["target_price_column_long"] = "high"
        kwargs["target_price_column_short"] = "low"
        kwargs["relative_stoploss_value"] = 10
        kwargs["relative_target_value"] = 80
        kwargs["indicator_fields"] = [{"field": "lowerPullback", "panel": 2},
                                      {"field": "upperPullback", "panel": 1},
                                      {"field": "LowerBreakouts", "panel": 3},
                                      "donchainUpper",
                                      "donchainMiddle",
                                      "donchainLower"]
        non_trading_timeslots = []
        non_trading_timeslots.extend(StrategyExecutor.NON_TRADING_FIRST_HOUR)
        non_trading_timeslots.extend(StrategyExecutor.NON_TRADING_AFTERNOON)
        kwargs["non_trading_timeslots"] = non_trading_timeslots
        self.breakout_threshold = 3
        self.max_sl = 40
        self.rratio = 3
        # kwargs["plot_results"] = False
        print(args, kwargs)
        super().__init__(*args, **kwargs)

    def get_entry(self, window: pd.DataFrame, trade_type: TradeType):
        if trade_type == TradeType.LONG:
            return window.iloc[-1]["high"] + self.breakout_threshold
        else:
            return window.iloc[-1]["low"] - + self.breakout_threshold

    def get_stoploss(self, window: pd.DataFrame, trade_type: TradeType) -> float:
        if trade_type == TradeType.LONG:
            return max(window.iloc[-1]["donchainMiddle"],
                       min(window.iloc[-1]["low"],
                       window.iloc[-1]["close"] - self.max_sl,
                       window.iloc[-1]["open"] - self.max_sl))
        else:
            if window.iloc[-1]["close"] > window.iloc[-1]["donchainMiddle"]:
                return max(window.iloc[-1]["high"],
                           window.iloc[-1]["close"] + self.max_sl,
                           window.iloc[-1]["open"] + self.max_sl)
            return min(window.iloc[-1]["donchainMiddle"],
                      max( window.iloc[-1]["high"],
                       window.iloc[-1]["close"] + self.max_sl,
                       window.iloc[-1]["open"] + self.max_sl))

    def __get_donchain_as_entry(self,
                                window: pd.DataFrame,
                                trade_type: TradeType) -> float:
        if trade_type == TradeType.LONG:
            return window.iloc[-1]["donchainUpper"] + self.breakout_threshold
        else:
            return window.iloc[-1]["donchainLower"] - self.breakout_threshold

    def get_target(self, window: pd.DataFrame, trade_type: TradeType) -> float:
        if trade_type == TradeType.LONG:
            return window.iloc[-1]["donchainUpper"] + self.max_sl + self.rratio
        else:
            return window.iloc[-1]["donchainLower"] - self.max_sl * self.rratio

    def strategy(self, window: pd.DataFrame) -> Optional[TradeType]:
        colvals = []
        for col in window.columns:
            if col not in ["open", "high", "low", "close"]:
                colvals.append(col)
                colvals.append(str(window.iloc[-1][col]))
        self.logger.info(f"Strategy: timestamp {window.iloc[-1].index.name}; OHLC"
                         f" {window.iloc[-1]['open']}"
                         f" {window.iloc[-1]['high']}"
                         f" {window.iloc[-1]['low']}"
                         f" {window.iloc[-1]['close']} "
                         f"{' '.join(colvals)}")
        
        donchain_breakout_order_active = False
        for order in self.trade_manager.get_orders():
            if (order.state == OrderState.PENDING
                and "donchain_pullback" in order.tags):
                donchain_breakout_order_active = True
                break

        if (not donchain_breakout_order_active):
            self.group_id = new_id()
            if window.iloc[-1]["upperPullback"] == 1.0:
                self.logger.info(f"1.0 LONG CASE")
                self.take_position(window,
                                   trade_type=TradeType.LONG,
                                   entry_price_func=self.__get_donchain_as_entry,
                                   tags=["donchain_pullback",
                                         "upper_pullback_breakout"],
                                   group_id=self.group_id)
                self.take_position(window,
                                   trade_type=TradeType.SHORT,
                                   tags=["donchain_pullback",
                                         "upper_pullback_breakout"],
                                   group_id=self.group_id)
            #elif window.iloc[-1]["upperPullback"] == 2.0:
            #    self.take_position(window,
            #                       trade_type=TradeType.SHORT,
            #                       sl_price_func=self.__get_donchain_as_stoploss,
            #                       tags=["donchain_pullback",
            #                             "upper_pullback_midline_swing"],
            #                       group_id=self.group_id)
            """
            elif window.iloc[-1]["lowerPullback"] > 0.0:
                self.take_position(window,
                                   trade_type=TradeType.LONG,
                                   tags=["donchain_pullback",
                                         "lower_pullback_breakout"],
                                   group_id=self.group_id)
                self.take_position(window,
                                   trade_type=TradeType.SHORT,
                                   tags=["donchain_pullback",
                                         "lower_pullback_breakout"],
                                   group_id=self.group_id)
            """
            #elif window.iloc[-1]["lowerPullback"] == 2.0:
            #    self.take_position(window,
            #                       trade_type=TradeType.SHORT,
            #                       sl_price_func=self.__get_donchain_as_stoploss,
            #                       tags=["donchain_pullback",
            #                             "lower_pullback_midline_swing"],
            #                       group_id=self.group_id)
        else:

            for order in self.trade_manager.get_orders():

                if (order.state == OrderState.PENDING
                    and "sl_order" in order.tags
                    and order.order_type == OrderType.SL_LIMIT):
                    if "donchain_pullback" in order.tags:
                        new_sl_price = None
                        if order.transaction_type == TransactionType.SELL:
                            diff = (window.iloc[-1]["low"] - order.trigger_price)
                            if diff >= self.max_sl:
                                new_sl_price = max(window.iloc[-1]["donchainMiddle"],
                                                   order.trigger_price + (diff - self.max_sl))
                        elif order.transaction_type == TransactionType.BUY:
                              diff = (order.trigger_price - window.iloc[-1]["high"])
                              if diff >= self.max_sl:
                                new_sl_price = min(window.iloc[-1]["donchainMiddle"],
                                                   order.trigger_price - (diff - self.max_sl))
                        if new_sl_price is not None:
                            order.trigger_price = new_sl_price
                            order.limit_price = new_sl_price
                            self.trade_manager.update_order(order)
                    """
                    elif "lower_pullback_breakout" in order.tags:
                        order.trigger_price = window.iloc[-4:-1]["high"].max()
                        order.limit_price = window.iloc[-1]["high"]    
                    """            
        return None
            