from typing import Optional
import datetime

import pandas as pd
import numpy as np

from ..core.ds import (TradeType, OrderType,
                       OrderState, TransactionType)
from ..core.util import new_id
from ..core.strategy import (StrategyExecutor)
from ..core.indicator import (IndicatorPipeline,
                              HeikinAshiIndicator,
                              CDLPatternIndicator,
                              SupportIndicator,
                              WMAIndicator,
                              SMAIndicator,
                              RSIIndicator,
                              SlopeIndicator,
                              ADXIndicator,
                              ATRIndicator,
                              DonchainIndicator,
                              PullbackDetector)



class WMASupportStrategy(StrategyExecutor):

    def __init__(self,
                 *args,
                 ma_period: int = 20,
                 long_ma_period: int = 200,
                 ma_type: str = "WMA",
                 donchain_period: int = 15,
                 rsi_period: int = 14,
                 adx_period: int = 14,
                 atr_period: int = 14,
                 **kwargs):
        self.ma_period = ma_period
        self.donchain_period = donchain_period
        self.ma_type = ma_type
        self.rsi_period = rsi_period
        self.adx_period = adx_period
        self.atr_period = atr_period
        self.long_ma_period = long_ma_period
        indicators = indicators=[(HeikinAshiIndicator(), None, None),
                                 (WMAIndicator(period=ma_period), None, None),
                                 (WMAIndicator(period=self.long_ma_period), None, None),
                                 (CDLPatternIndicator(period=ma_period), {"CDLHAMMER": "CDLHAMMER",
                                                                          "CDLHANGINGMAN": "CDLHANGINGMAN",
                                                                          "CDLSHOOTINGSTAR": "CDLSHOOTINGSTAR",
                                                                          "CDLINVERTEDHAMMER": "CDLINVERTEDHAMMER", "CDLENGULFING": "CDLENGULFING"}, None),
                                 (DonchainIndicator(period=self.donchain_period), None, None),
                                 (SupportIndicator(direction=SupportIndicator.SUPPORT_DIRECTION_UP,
                                                   factor = 0.08/100,
                                                   signal=f"{self.ma_type}_{self.ma_period}"), None, None),
                                (SupportIndicator(direction=SupportIndicator.SUPPORT_DIRECTION_DOWN,
                                                   factor = 0.08/100,
                                                   signal=f"{self.ma_type}_{self.ma_period}"), None, None),
                                 (ADXIndicator(period=self.adx_period), None, None),
                                 (RSIIndicator(period=self.rsi_period), None, None),
                                 (ATRIndicator(period=self.atr_period), None, None),
                                 (SlopeIndicator(signal=f"{self.ma_type}_{self.ma_period}"), None, None),
                                 (SMAIndicator(signal=f"{self.ma_type}_{self.ma_period}_slope", period=20), None, None)]
        kwargs["indicator_pipeline"] = IndicatorPipeline(indicators=indicators)
        kwargs["long_entry_price_column"] = "high"
        kwargs["short_entry_price_column"] = "low"
        kwargs["sl_price_column_long"] = "high"
        kwargs["sl_price_column_short"] = "low"
        kwargs["target_price_column_long"] = "high"
        kwargs["target_price_column_short"] = "low"
        kwargs["relative_stoploss_value"] = 10
        kwargs["relative_target_value"] = 80
        kwargs["indicator_fields"] = [{"field": f"{self.ma_type}_{self.ma_period}_up_support", "panel": 2},
                                      {"field": f"{self.ma_type}_{self.ma_period}_down_support", "panel": 2},
                                      #{"field": "CDLHAMMER", "panel": 3},
                                      #{"field": f"CDLENGULFING", "panel": 3},
                                      #{"field": f"RSI_{self.rsi_period}", "panel": 3},
                                      {"field": f"SMA_20_{self.ma_type}_{self.ma_period}_slope", "panel": 3},
                                      #{"field": f"RSI_{self.rsi_period}", "panel": 4},
                                      # {"field": "CDLSHOOTINGSTAR", "panel": 3},
                                      #{"field": "CDLHANGINGMAN", "panel": 3},
                                      f"{self.ma_type}_{self.ma_period}",
                                      f"donchainUpper_{self.donchain_period}",
                                      f"donchainLower_{self.donchain_period}",
                                      #f"_support_zone_upper",
                                      #f"_support_zone_lower",
                                      f"{self.ma_type}_{self.long_ma_period}"]
        non_trading_timeslots = []
        #non_trading_timeslots.extend(StrategyExecutor.NON_TRADING_FIRST_HOUR)
        non_trading_timeslots.extend(StrategyExecutor.NON_TRADING_AFTERNOON)
        kwargs["non_trading_timeslots"] = non_trading_timeslots
        
        # self.target_amt = 10 # DRREDDY
        # HDFC BANK
        # self.breakout_threshold = 0.5
        # self.target_amt = 20
        # self.rratio = 2
        # AXIS BANK
        # self.target_amt = 5 
        # self.rratio = 2
        # self.breakout_threshold = 0.5
        # NIFTY 50
        self.entry_threshold = 2
        self.stoploss_threshold = 5
        self.target_amt = 160
        self.rratio = 2
        print(args, kwargs)
        super().__init__(*args, **kwargs)

    def get_entry(self, window: pd.DataFrame, trade_type: TradeType):
        if trade_type == TradeType.LONG:
            return max(window.iloc[-1][f"{self.ma_type}_{self.ma_period}"], window.iloc[-1]["high"]) + self.entry_threshold
        else:
            return min(window.iloc[-1][f"{self.ma_type}_{self.ma_period}"], window.iloc[-1]["low"]) - self.entry_threshold

    def get_stoploss(self, window: pd.DataFrame, trade_type: TradeType) -> float:
        if trade_type == TradeType.LONG:
            return min(window.iloc[-1][f"{self.ma_type}_{self.ma_period}"], window.iloc[-1]["low"]) - self.entry_threshold
        else:
            return max(window.iloc[-1][f"{self.ma_type}_{self.ma_period}"], window.iloc[-1]["high"]) + self.entry_threshold
        """
        if trade_type == TradeType.LONG:
            return min(window.iloc[-1][f"_support_zone_lower"], window.iloc[-1]["low"]) - self.stoploss_threshold
        else:
            return max(window.iloc[-1][f"_support_zone_upper"], window.iloc[-1]["high"]) + self.stoploss_threshold
        """

    def get_target(self, window: pd.DataFrame, trade_type: TradeType) -> float:
        if trade_type == TradeType.LONG:
            return self.get_entry(window, trade_type) + self.rratio * (self.get_entry(window, trade_type) - self.get_stoploss(window, trade_type))
        else:
            return self.get_entry(window, trade_type) - self.rratio * (self.get_stoploss(window, trade_type) - self.get_entry(window, trade_type))
        """
        if trade_type == TradeType.LONG:
            #return window.iloc[-1][f"donchainUpper_{self.donchain_period}"]
            return max(window.iloc[-1][f"donchainUpper_{self.donchain_period}"], window.iloc[-1]["high"] + self.target_amt) # 5 #+ 15 #self.rratio * (window.iloc[-1]["high"] - window.iloc[-1]["low"])
        else:
            #return window.iloc[-1][f"donchainLower_{self.donchain_period}"]
            return min(window.iloc[-1][f"donchainLower_{self.donchain_period}"], window.iloc[-1]["low"] - self.target_amt) # 5 #- 15 #self.rratio * (window.iloc[-1]["high"] - window.iloc[-1]["low"])
        """

    def candle_size(self, window, idx):
        return abs(window.iloc[idx]["close"] - window.iloc[idx]["open"])

    def cancel_active_order(self):
        for order in self.trade_manager.get_orders():
            if (order.state == OrderState.PENDING
                and f"{self.ma_type}_support_{self.ma_period}" in order.tags
                and f"entry_order" in order.tags):
               self.trade_manager.cancel_order(order)
        self.perform_squareoff()
       

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
                         f" {window.iloc[-1]['close']} "
                         f"{' '.join(colvals)}")
        """
        for order in self.trade_manager.get_orders():
            if (order.state == OrderState.PENDING
                and f"{self.ma_type}_support_{self.ma_period}" in order.tags
                and f"entry_order" in order.tags):
                tdiff = self.trade_manager.current_datetime() - order.timestamp
                self.logger.info(f"Found order with tdiff {tdiff}")
                if tdiff >= datetime.timedelta(minutes=15):
                    self.logger.info(f"Cancelling order as it has not been fulfilled. {tdiff}")
                    self.trade_manager.cancel_order(order)
                    continue
       """
        active_order = False
        for order in self.trade_manager.get_orders():
            if (order.state == OrderState.PENDING
                and f"{self.ma_type}_support_{self.ma_period}" in order.tags
                and not f"entry_order" in order.tags):
                active_order = True
                break
        
        """
        if active_order:
            for entry_order, order in self.trade_manager.get_gtt_orders():
                if (order.state == OrderState.PENDING
                    and f"{self.ma_type}_support_{self.ma_period}" in order.tags
                    and f"target_order" in order.tags):
                    if order.transaction_type == TransactionType.BUY:
                        order.trigger_price = window.iloc[-1][f"donchainLower_{self.donchain_period}"]
                        order.limit_price = window.iloc[-1][f"donchainLower_{self.donchain_period}"]
                    else:
                        order.trigger_price = window.iloc[-1][f"donchainUpper_{self.donchain_period}"]
                        order.limit_price = window.iloc[-1][f"donchainUpper_{self.donchain_period}"]
                    self.trade_manager.update_gtt_order(entry_order, order)
        for order in self.trade_manager.get_orders():
            if (order.state == OrderState.PENDING
                and f"{self.ma_type}_support_{self.ma_period}" in order.tags
                and f"sl_order" in order.tags):
                changed = False
                if order.transaction_type == TransactionType.BUY:
                    if window.iloc[-1]["low"] <= window.iloc[-2][f"donchainLower_{self.donchain_period}"]:
                        changed = True
                        order.trigger_price = window.iloc[-1]["high"] + self.stoploss_threshold
                        order.limit_price = order.trigger_price
                    elif (window.iloc[-1]["close"] > window.iloc[-1]["open"]
                          and self.candle_size(window, -1) >  self.candle_size(window, -2)
                          and self.candle_size(window, -1) > 20):
                        changed = True
                        order.trigger_price = window.iloc[-1]["high"] + self.stoploss_threshold
                        order.limit_price = order.trigger_price
                else:
                    if window.iloc[-1]["high"] >= window.iloc[-2][f"donchainUpper_{self.donchain_period}"]:
                        changed = True
                        order.trigger_price = window.iloc[-1]["low"] - self.stoploss_threshold
                        order.limit_price = order.trigger_price
                    elif (window.iloc[-1]["close"] < window.iloc[-1]["open"]
                          and self.candle_size(window, -1) > self.candle_size(window, -2)
                          and self.candle_size(window, -1) > 20):
                        changed = True
                        order.trigger_price = window.iloc[-1]["low"] - self.stoploss_threshold
                        order.limit_price = order.trigger_price
                if changed:
                    self.trade_manager.update_order(order)
        """
        if self.can_trade(window):
            #if window.iloc[-1][f"ADX"] > 19.8 or abs(window.iloc[-1][f"{self.ma_type}_{self.ma_period}_slope"] > 1.5):
            #if (window.iloc[-1][f"donchainUpper_{self.donchain_period}"] - window.iloc[-1][f"donchainLower_{self.donchain_period}"] < 50):
                #or abs(window.iloc[-1][f"SMA_20_{self.ma_type}_{self.long_ma_period}_slope"]) < 0.3):
            #    return None
            if window.iloc[-1][f"{self.ma_type}_{self.ma_period}_up_support"] == 1.0: # and window.iloc[-1][f"ATR_{self.atr_period}"] > 15: # and window.iloc[-1][f"ADX_{self.adx_period}_slope"] >= 0.5
                if window.iloc[-1][f"SMA_20_{self.ma_type}_{self.ma_period}_slope"] >= 0.5:
                    if window.iloc[-1][f"{self.ma_type}_{self.long_ma_period}"] < window.iloc[-1]["close"]:
                        if window.iloc[-1][f"{self.ma_type}_{self.long_ma_period}"] < window.iloc[-1][f"donchainLower_{self.donchain_period}"]:
                            self.cancel_active_order()
                            self.take_position(window, trade_type=TradeType.LONG, tags=[f"{self.ma_type}_support_{self.ma_period}"])
            elif window.iloc[-1][f"{self.ma_type}_{self.ma_period}_down_support"] == 1.: #and window.iloc[-1][f"ATR_{self.atr_period}"] > 15: #  and window.iloc[-1][f"ADX_{self.adx_period}_slope"] <= 0.5
                if window.iloc[-1][f"SMA_20_{self.ma_type}_{self.ma_period}_slope"] <= -0.5:
                    if window.iloc[-1][f"{self.ma_type}_{self.long_ma_period}"] > window.iloc[-1]["close"]:
                        if window.iloc[-1][f"{self.ma_type}_{self.long_ma_period}"] > window.iloc[-1][f"donchainUpper_{self.donchain_period}"]:
                            self.cancel_active_order()
                            self.take_position(window, trade_type=TradeType.SHORT, tags=[f"{self.ma_type}_support_{self.ma_period}"])

        return None
