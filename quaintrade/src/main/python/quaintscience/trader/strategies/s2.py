from __future__ import annotations
from typing import Optional
from collections import OrderedDict
import datetime
from enum import Enum
import pandas as pd
import numpy as np
from ..core.ds import (Order,
                       TradeType,
                       OrderState,
                       PositionType,
                       TradingProduct)

from ..core.strategy import Strategy
from ..core.indicator import (IndicatorPipeline,
                              ATRIndicator,
                              DonchianIndicator,
                              BreakoutDetector,
                              MAIndicator,
                              RSIIndicator,
                              PostBreakoutCrossDetector)

from ..core.roles import Broker
from ..core.statemachine import TradingStateMachine, Action

from ..core.util import (new_id,
                         get_key_from_scrip_and_exchange,
                         is_monotonically_increasing,
                         is_monotonically_decreasing,
                         is_local_maxima,
                         is_local_minima,
                         span,
                         sameday,
                         get_pivot_value)


class S2(Strategy):

    def __init__(self,
                 *args,
                 atr_period: int = 14,
                 dc_period: int = 15,
                 ma_period: int = 100,
                 rsi_period: int = 14,
                 product: TradingProduct = TradingProduct.MIS,
                 **kwargs):
        self.atr_period = atr_period
        self.atr_indicator = ATRIndicator(period=self.atr_period)
        self.atr_col = self.atr_indicator.get_default_column_names()["ATR"]
        self.ma_period = ma_period
        self.ma_indicator = MAIndicator(period=self.ma_period, signal="close", ma_type="EMA")
        self.ma_col = self.ma_indicator.get_default_column_names()["MA"]
        self.dc_period = dc_period
        self.dc_indicator = DonchianIndicator(period=self.dc_period)
        self.dc_col_upper = self.dc_indicator.get_default_column_names()["upper"]
        self.dc_col_lower = self.dc_indicator.get_default_column_names()["lower"]
        self.dc_col_basis = self.dc_indicator.get_default_column_names()["basis"]

        self.dc_up_breakout_indicator = BreakoutDetector(direction=BreakoutDetector.BREAKOUT_DIRECTION_UP,
                                                         threshold_signal=self.dc_col_upper,
                                                         signal="high")
        self.dc_down_breakout_indicator = BreakoutDetector(direction=BreakoutDetector.BREAKOUT_DIRECTION_DOWN,
                                                           threshold_signal=self.dc_col_lower,
                                                           signal="low")
        
        self.dc_upper_breakout_col = self.dc_up_breakout_indicator.get_default_column_names()["breakout"]
        self.dc_lower_breakout_col = self.dc_down_breakout_indicator.get_default_column_names()["breakout"]


        self.post_breakout_cross_indicator = PostBreakoutCrossDetector(condition_signals=[self.dc_upper_breakout_col,
                                                                                          self.dc_lower_breakout_col],
                                                                      negation_signal=self.dc_col_basis)

        self.entry_points_col = self.post_breakout_cross_indicator.get_default_column_names()["signal"]
        
        self.rsi_period = rsi_period
        self.rsi_indicator = RSIIndicator(period=self.rsi_period)
        self.rsi_col = self.rsi_indicator.get_default_column_names()["RSI"]

        indicators =[
                    (self.ma_indicator, None, None),
                    (self.rsi_indicator, None, None),
                    (self.atr_indicator, None, None),
                    (self.dc_indicator, None, None),
                    (self.dc_up_breakout_indicator, None, None),
                    (self.dc_down_breakout_indicator, None, None),
                    (self.post_breakout_cross_indicator, None, None),
                    ]
        indicators = IndicatorPipeline(indicators)

        kwargs["indicator_pipeline"] = {"window": indicators,
                                        "context": {}}
        
        kwargs["plottables"] = {"indicator_fields": [{"field": self.ma_col,
                                                      "color": "magenta",
                                                      "panel": 0},
                                                     {"field": self.dc_col_upper,
                                                      "color": "green",
                                                      "panel": 0},
                                                      {"field": self.dc_col_lower,
                                                      "color": "red",
                                                      "panel": 0},
                                                      {"field": self.dc_col_basis,
                                                      "color": "black",
                                                      "panel": 0},
                                                      {"field": self.atr_col,
                                                      "color": "blue",
                                                      "panel": 2},
                                                     {"field": self.dc_upper_breakout_col,
                                                      "color": "green",
                                                      "panel": 3},
                                                      {"field": self.dc_lower_breakout_col,
                                                      "color": "red",
                                                      "panel": 3},
                                                      {"field": self.entry_points_col,
                                                      "color": "black",
                                                      "panel": 3},
                                                     ]}

        kwargs["plot_context_candles"] = []
        non_trading_timeslots = []
        #non_trading_timeslots.extend(Strategy.NON_TRADING_FIRST_HOUR)
        non_trading_timeslots.extend([{"from": {"hour": 9,
                                       "minute": 15},
                                       "to": {"hour": 9,
                                       "minute": 20}}])
        non_trading_timeslots.extend([{"from": {"hour": 13,
                                       "minute": 00},
                                       "to": {"hour": 15,
                                       "minute": 59}}])
        kwargs["non_trading_timeslots"] = []
        kwargs["context_required"] = []
    
        kwargs["intraday_squareoff"] = True
        kwargs["squareoff_hour"] = 15
        kwargs["squareoff_minute"] = 5


        self.target_factor = 2
        self.slippage = 0.01
        # self.max_risk = 2.
        self.product = TradingProduct.MIS
        super().__init__(*args, **kwargs)

    def apply_impl(self,
                broker: Broker,
                scrip: str,
                exchange: str,
                window: pd.DataFrame,
                context: dict[str, pd.DataFrame]) -> Optional[TradeType]:

        current_run = self.get_current_run(broker=broker,
                                           scrip=scrip,
                                           exchange=exchange)



        if self.can_trade(window, context):
            qty = max(self.max_budget // window.iloc[-1]["close"],
                      self.min_quantity)
            atr = window.iloc[-1][self.atr_col]
            rsi = window.iloc[-1][self.rsi_col]
            ma_increasing = is_monotonically_increasing(window[self.ma_col].iloc[-2:])
            ma_decreasing = is_monotonically_decreasing(window[self.ma_col].iloc[-2:])
            ma_above_price = window.iloc[-1][self.ma_col] > window.iloc[-1]["low"]
            ma_below_price = window.iloc[-1][self.ma_col] < window.iloc[-1]["high"]
            
            next_trade = None
            dc_width = window.iloc[-1][self.dc_col_upper] - window.iloc[-1][self.dc_col_lower]
            if (current_run != TradeType.LONG
                and window.iloc[-1][self.entry_points_col] == 1.0
                #and dc_width > 75
                and (ma_increasing
                     or rsi < 40)
                
               ):
                next_trade = TradeType.LONG
                entry_price = window.iloc[-1]["high"] #+ 0.05 * atr
                sl_price = max(window.iloc[-2]["low"], window.iloc[-1]["high"] - atr)
                target_price = entry_price + 3 * (entry_price - sl_price)
            #elif (current_run != TradeType.SHORT
            #      and window.iloc[-1][self.entry_points_col] == -1.0
            #      #and dc_width > 75
            #      and (ma_decreasing
            #           or rsi > 60)
            #      ):
            #    next_trade = TradeType.SHORT
            #    entry_price = window.iloc[-1]["low"] #+ 0.05 * atr
            #    sl_price = window.iloc[-2]["high"] #- 0.05 * atr
            #    target_price = entry_price + 3 * (entry_price - sl_price)

            if next_trade is not None:

                gid = new_id()
                entry_order = self.take_position(scrip=scrip,
                                                    exchange=exchange,
                                                    broker=broker,
                                                    position_type=PositionType.ENTRY,
                                                    trade_type=next_trade,
                                                    trigger_price=entry_price,
                                                    limit_price=entry_price,
                                                    quantity=qty,
                                                    product=self.product,
                                                    group_id=gid)

                if entry_order is None:
                    print("Placing order failed. "
                            "skipping gtts; this happens if price"
                            " movement is too fast.")
                
                else:
                    self.take_position(scrip=scrip,
                                        exchange=exchange,
                                        broker=broker,
                                        position_type=PositionType.STOPLOSS,
                                        trade_type=next_trade,
                                        trigger_price=sl_price,
                                        limit_price=sl_price,
                                        quantity=qty,
                                        product=self.product,
                                        parent_order=entry_order)
                    self.take_position(scrip=scrip,
                                        exchange=exchange,
                                        broker=broker,
                                        position_type=PositionType.TARGET,
                                        trade_type=next_trade,
                                        limit_price=target_price,
                                        quantity=qty,
                                        product=self.product,
                                        parent_order=entry_order)
