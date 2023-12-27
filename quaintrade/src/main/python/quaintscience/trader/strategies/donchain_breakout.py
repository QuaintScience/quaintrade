from typing import Optional

import pandas as pd
import numpy as np

from ..core.ds import TradeType, OrderType, TransactionType, OrderState, CandleType
from ..core.util import new_id
from ..core.strategy import (StrategyExecutor,
                             CandleBasedPriceEntryMixin,
                             RelativeStopLossAndTargetMixin)
from ..core.indicator import (IndicatorPipeline,
                              HeikinAshiIndicator,
                              DonchainIndicator,
                              BreakoutIndicator,
                              ADXIndicator,
                              RSIIndicator)



class DonchainBreakoutStrategy(RelativeStopLossAndTargetMixin,
                               CandleBasedPriceEntryMixin,
                               StrategyExecutor):

    def __init__(self, *args, **kwargs):
        indicators = indicators=[(HeikinAshiIndicator(), None, None),
                                 (DonchainIndicator(period=50), None, None),
                                 (ADXIndicator(), None, None),
                                 (RSIIndicator(), None, None),
                                 (BreakoutIndicator(upper_breakout_column="donchainUpper",
                                                   lower_breakout_column="donchainLower",
                                                   data_interval="10min"), None, None)]
        kwargs["indicator_pipeline"] = IndicatorPipeline(indicators=indicators)
        kwargs["long_entry_price_column"] = "high"
        kwargs["short_entry_price_column"] = "low"
        kwargs["sl_price_column_long"] = "high"
        kwargs["sl_price_column_short"] = "low"
        kwargs["target_price_column_long"] = "high"
        kwargs["target_price_column_short"] = "low"
        kwargs["relative_stoploss_value"] = 10
        kwargs["relative_target_value"] = 80
        kwargs["indicator_fields"] = ["donchainUpper",
                                      "donchainLower",
                                      "donchainMiddle",
                                      {"field": "donchainUpper_breakout",
                                       "panel": 1},
                                       {"field": "donchainLower_breakout",
                                       "panel": 1},
                                       {"field": "RSI",
                                       "panel": 2},]
        non_trading_timeslots = []
        non_trading_timeslots.extend(StrategyExecutor.NON_TRADING_FIRST_HOUR)
        non_trading_timeslots.extend(StrategyExecutor.NON_TRADING_AFTERNOON)
        kwargs["non_trading_timeslots"] = non_trading_timeslots
        self.trail_threshold = 5
        self.breakout_threshold = 1
        self.max_sl = 50
        self.rratio = 1
        #kwargs["plot_results"] = False
        print(args, kwargs)
        super().__init__(*args, **kwargs)

    def __donchain_entry_price(self, window: pd.DataFrame,
                               trade_type: TradeType):
        if trade_type == TradeType.LONG:
            return window.iloc[-1]["donchainUpper"] + self.breakout_threshold
        else:
            return window.iloc[-1]["donchainLower"] - self.breakout_threshold

    def __donchain_sl_price(self, window: pd.DataFrame,
                            trade_type: TradeType):

        if trade_type == TradeType.LONG:
            return window.iloc[-1]["donchainMiddle"]
            
        else:
            return window.iloc[-1]["donchainMiddle"]
        #    return min(window.iloc[-1]["donchainMiddle"],
        #               window.iloc[-1]["high"])

    def __donchain_target_price(self, window: pd.DataFrame,
                                trade_type: TradeType):
        if trade_type == TradeType.LONG:
            return window.iloc[-1]["donchainUpper"] + (self.rratio * self.max_sl)
        else:
            return window.iloc[-1]["donchainLower"] - (self.rratio * self.max_sl)

    def strategy(self, window: pd.DataFrame,
                 context: dict[str, pd.DataFrame]) -> Optional[TradeType]:
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
        donchain_breakout_order_active = False
        for order in self.trade_manager.get_orders():
            if (order.state == OrderState.PENDING
                and "donchain_breakout" in order.tags):
                donchain_breakout_order_active = True
                break

        if (not donchain_breakout_order_active
            and not np.isnan(window.iloc[-1]["donchainUpper"])):
            self.group_id = new_id()
            self.take_position(window,
                               trade_type=TradeType.LONG,
                               entry_price_func=self.__donchain_entry_price,
                               sl_price_func=self.__donchain_sl_price,
                               target_price_func=self.__donchain_target_price,
                               tags=["donchain_breakout"],
                               group_id=self.group_id)
            self.take_position(window,
                               trade_type=TradeType.SHORT,
                               entry_price_func=self.__donchain_entry_price,
                               sl_price_func=self.__donchain_sl_price,
                               target_price_func=self.__donchain_target_price,
                               tags=["donchain_breakout"],
                               group_id=self.group_id)
        else:

            for order in self.trade_manager.get_orders():
                if "donchain_breakout" not in order.tags:
                    continue
                if (order.group_id == self.group_id
                    and "entry_order" in order.tags
                    and order.state == OrderState.PENDING):
                    new_limit_price = None
                    trade_type = None
                    if order.transaction_type == TransactionType.SELL:
                        trade_type = TradeType.SHORT
                    else:
                        trade_type = TradeType.LONG

                    
                    new_limit_price = self.__donchain_entry_price(window, 
                                                                  trade_type=trade_type)
                    if new_limit_price != order.limit_price:
                        order.limit_price = new_limit_price
                        order.trigger_price = order.limit_price
                        self.trade_manager.update_order(order)

                        # Update SL and target orders
                        other_orders = self.trade_manager.get_gtt_orders_for(order)
                        for other_order in other_orders:
                            other_order_changed = False
                            if "sl_order" in other_order.tags:
                                other_order.limit_price = self.__donchain_sl_price(window, trade_type)
                                other_order.trigger_price = other_order.limit_price
                                other_order_changed = True
                            elif "target_order" in other_order.tags:
                                other_order.limit_price = self.__donchain_target_price(window, trade_type)
                                other_order_changed = True
                            if other_order_changed:
                                self.trade_manager.update_gtt_order(order, other_order)
                elif ("sl_order" in order.tags
                      and order.state == OrderState.PENDING):

                    trade_type = None
                    if order.transaction_type == TransactionType.SELL:
                        trade_type = TradeType.SHORT
                    else:
                        trade_type = TradeType.LONG

                    order.trigger_price = self.__donchain_sl_price(window, trade_type)
                    order.limit_price = order.trigger_price
                    self.trade_manager.update_order(order)

        #if window.iloc[-1]["donchainUpper_breakout"]:
        #    return TradeType.LONG
        #if window.iloc[-1]["donchainLower_breakout"]:
        #    return TradeType.SHORT
