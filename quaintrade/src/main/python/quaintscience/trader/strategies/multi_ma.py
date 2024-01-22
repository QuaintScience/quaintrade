from typing import Optional
from collections import defaultdict

import pandas as pd
import numpy as np

from ..core.ds import (TradeType, OrderState, PositionType, TradingProduct)

from ..core.strategy import Strategy
from ..core.indicator import (IndicatorPipeline,
                              ATRIndicator,
                              RSIIndicator,
                              WMAIndicator)
from ..core.roles import Broker
from ..core.util import new_id


class MultiMAStrategy(Strategy):

    def __init__(self,
                 *args,
                 rsi_period: int = 14,
                 atr_period: int = 3,
                 long_ma: int = 200,
                 short_ma: int = 20,
                 product: TradingProduct = TradingProduct.MIS,
                 **kwargs):
        self.rsi_period = rsi_period
        self.atr_period = atr_period
        self.long_ma = long_ma
        self.short_ma = short_ma
        self.product = product
        indicators = indicators=[(RSIIndicator(period=self.rsi_period), None, None),
                                 (ATRIndicator(period=self.atr_period), None, None),
                                 (WMAIndicator(period=self.long_ma), None, None),
                                 (WMAIndicator(period=self.short_ma), None, None)]
        self.long_ma_col = f"WMA_{self.long_ma}"
        self.short_ma_col = f"WMA_{self.short_ma}"

        kwargs["indicator_pipeline"] = IndicatorPipeline(indicators=indicators)
        kwargs["plottables"] = {"indicator_fields": [{"field": f"RSI_{self.rsi_period}", "panel": 3},
                                                     {"field": f"ATR_{self.atr_period}", "panel": 4},
                                                     self.short_ma_col,
                                                     self.long_ma_col]}
        
        non_trading_timeslots = []
        non_trading_timeslots.extend([{"from": {"hour": 9,
                                        "minute": 0},
                                      "to": {"hour": 9,
                                      "minute": 25}}])
        #non_trading_timeslots.extend(Strategy.NON_TRADING_AFTERNOON)
        #kwargs["non_trading_timeslots"] = non_trading_timeslots
        kwargs["context_required"] = []
        #kwargs["squareoff_hour"] = 15
        #kwargs["squareoff_minute"] = 0
        kwargs["intraday_squareoff"] = True
        kwargs["trigger_price_cushion"] = 0.00001
        self.rsi_col = f"RSI_{self.rsi_period}"
        self.atr_col = f"ATR_{self.atr_period}"
        
        self.entry_threshold = 0.05
        """
        self.sl_factor = 2
        self.target_factor = 2.5
        """

        self.sl_factor = 0.1
        self.target_factor = 3


        self.risk_ratio = 2
        self.bar_cnt = defaultdict(int)
        
        self.rsi_upper_threshold = 60
        self.rsi_lower_threshold = 40

        self.long_cross_over = False
        self.short_cross_over = False
        self.rsi_lower_threshold_breach = False
        self.rsi_upper_threshold_breach = False
        self.current_run = None
        """
        self.sl_factor = 3
        self.target_factor = 3
        """

        print(args, kwargs)
        super().__init__(*args, **kwargs)
    """
    def get_entry(self, window: pd.DataFrame, context: dict[str, pd.DataFrame],  trade_type: TradeType):
        if trade_type == TradeType.LONG:
            return window.iloc[-1]["high"] + window.iloc[-1][self.atr_col] * self.atr_factor
        else:
            return window.iloc[-1]["low"] - window.iloc[-1][self.atr_col] * self.atr_factor

    def get_target(self, window: pd.DataFrame, context: dict[str, pd.DataFrame], trade_type: TradeType):
        
        if trade_type == TradeType.LONG:
            return window.iloc[-1]["high"] + self.risk_ratio * abs(self.get_entry(window, context, trade_type) - self.get_stoploss(window, context, trade_type))
        else:
            return window.iloc[-1]["low"] - self.risk_ratio * abs(self.get_stoploss(window, context, trade_type) - self.get_entry(window, context, trade_type))

    def get_stoploss(self, window: pd.DataFrame, context: dict[str, pd.DataFrame], trade_type: TradeType):
        if trade_type == TradeType.LONG:
            return window.iloc[-1]["low"] - window.iloc[-1][self.atr_col] * self.sl_atr_factor
        else:
            return window.iloc[-1]["high"] + window.iloc[-1][self.atr_col] * self.sl_atr_factor
    """
    def get_entry(self, window: pd.DataFrame, context: dict[str, pd.DataFrame], trade_type: TradeType):
        if trade_type == TradeType.LONG:
            return window.iloc[-1]["high"] + (self.entry_threshold * window.iloc[-1][self.atr_col])
        else:
            return window.iloc[-1]["low"] - (self.entry_threshold * window.iloc[-1][self.atr_col])

    def get_target(self, window: pd.DataFrame, context: dict[str, pd.DataFrame], trade_type: TradeType):
        """
        if trade_type == TradeType.LONG:
            return window.iloc[-1]["high"] + self.risk_ratio * abs(self.get_entry(window, trade_type) - self.get_stoploss(window, trade_type))
        else:
            return window.iloc[-1]["low"] - self.risk_ratio * abs(self.get_stoploss(window, trade_type) - self.get_entry(window, trade_type))
        """

        if trade_type == TradeType.LONG:
            return window.iloc[-1]["high"] + self.target_factor * window.iloc[-1][self.atr_col]
        else:
            return window.iloc[-1]["low"] - self.target_factor * window.iloc[-1][self.atr_col]

    def get_stoploss(self, window: pd.DataFrame, context: dict[str, pd.DataFrame], trade_type: TradeType):
        
        if trade_type == TradeType.LONG:
            return window.iloc[-1]["low"] - self.sl_factor * window.iloc[-1][self.atr_col]
        else:
            return window.iloc[-1]["high"] + self.sl_factor * window.iloc[-1][self.atr_col]
        
    def apply_impl(self,
                   broker: Broker,
                   scrip: str,
                   exchange: str,
                   window: pd.DataFrame,
                   context: dict[str, pd.DataFrame]) -> Optional[TradeType]:

        colvals = []
        for col in window.columns:
            if col not in ["open", "high", "low", "close"]:
                colvals.append(f"{col}={window.iloc[-1][col]}")
        self.logger.info(f"{self.__class__.__name__} [{window.iloc[-1].name}]:"
                         f" O={window.iloc[-1]['open']}"
                         f" H={window.iloc[-1]['high']}"
                         f" L={window.iloc[-1]['low']}"
                         f" C={window.iloc[-1]['close']}"
                         f" {' '.join(colvals)}")

        if self.can_trade(window, context):
            trade_type = None
            current_run = self.get_current_run(broker, scrip=scrip, exchange=exchange)
            do_squareoff = False
            self.logger.info(f"Current Run: {current_run} ; "
                             f" long_crossover: {self.long_cross_over}"
                             f" short_crossover: {self.short_cross_over}"
                             f" upper_break: {self.rsi_upper_threshold_breach}"
                             f" lower_break: {self.rsi_lower_threshold_breach}")
            if (window.iloc[-2][self.long_ma_col] > window.iloc[-2][self.short_ma_col]
                and window.iloc[-1][self.long_ma_col] <= window.iloc[-1][self.short_ma_col]):
                #if current_run != TradeType.LONG: # Cross up of short MA - Long
                self.long_cross_over = True
                self.short_cross_over = False
                self.rsi_lower_threshold_breach = False
                self.rsi_upper_threshold_breach = False
                do_squareoff = True
            elif (window.iloc[-2][self.long_ma_col] < window.iloc[-2][self.short_ma_col]
                 and window.iloc[-1][self.long_ma_col] >= window.iloc[-1][self.short_ma_col]):
                #if current_run != TradeType.SHORT: # Cross down of short MA - Short
                self.long_cross_over = False
                self.short_cross_over = True
                self.rsi_lower_threshold_breach = False
                self.rsi_upper_threshold_breach = False
                do_squareoff = True

            if (self.long_cross_over and window.iloc[-1][self.rsi_col] <= self.rsi_lower_threshold):
                self.rsi_lower_threshold_breach = True
                self.rsi_upper_threshold_breach = False
            elif (self.short_cross_over and window.iloc[-1][self.rsi_col] >= self.rsi_upper_threshold):
                self.rsi_upper_threshold_breach = True
                self.rsi_lower_threshold_breach = False

            if (self.rsi_lower_threshold_breach
                and self.long_cross_over
                and window.iloc[-1]["close"] > window.iloc[-1]["open"]
                and current_run != TradeType.LONG
                #and window.iloc[-2][self.short_ma_col] < window.iloc[-1][self.short_ma_col] 
                ):
                trade_type = TradeType.LONG
            elif (self.rsi_upper_threshold_breach
                  and self.short_cross_over
                  and window.iloc[-1]["close"] < window.iloc[-1]["open"]
                  and current_run != TradeType.SHORT
                  #and window.iloc[-2][self.short_ma_col] > window.iloc[-1][self.short_ma_col]
                  ):
                trade_type = TradeType.SHORT
            
            if do_squareoff:
                quantity = self.cancel_active_orders(broker=broker,
                                                     scrip=scrip,
                                                     exchange=exchange,
                                                     product=self.product)
                self.perform_squareoff(broker=broker,
                                       scrip=scrip,
                                       exchange=exchange,
                                       quantity=quantity)

            if (trade_type is not None and
                not (np.isnan(self.get_entry(window, context, trade_type))
                     or np.isnan(self.get_stoploss(window, context, trade_type))
                     or np.isnan(self.get_target(window, context, trade_type)))):
                #self.short_cross_over = False
                #self.long_cross_over = False
                self.rsi_upper_threshold_breach = False
                self.rsi_lower_threshold_breach = False

                qty = max(self.max_budget // window.iloc[-1]["close"], 50)
                self.logger.debug(f"Taking position {trade_type}!")
                quantity = self.cancel_active_orders(broker=broker,
                                                     scrip=scrip,
                                                     exchange=exchange,
                                                     product=self.product)
                self.perform_squareoff(broker=broker,
                                       scrip=scrip,
                                       exchange=exchange,
                                       quantity=quantity)

                entry_order = self.take_position(scrip=scrip,
                                                 exchange=exchange,
                                                 broker=broker,
                                                 position_type=PositionType.ENTRY,
                                                 trade_type=trade_type,
                                                 price=self.get_entry(window, context, trade_type),
                                                 quantity=qty,
                                                 product=self.product)
                if entry_order is None:
                    print(f"Placing order failed. skipping gtts; this happens if price movement is too fast.")
                else:
                    self.take_position(scrip=scrip,
                                    exchange=exchange,
                                    broker=broker,
                                    position_type=PositionType.STOPLOSS,
                                    trade_type=trade_type,
                                    price=self.get_stoploss(window, context, trade_type),
                                    quantity=qty,
                                    product=self.product,
                                    parent_order=entry_order)
                    self.take_position(scrip=scrip,
                                    exchange=exchange,
                                    broker=broker,
                                    position_type=PositionType.TARGET,
                                    trade_type=trade_type,
                                    price=self.get_target(window, context, trade_type),
                                    quantity=qty,
                                    product=self.product,
                                    parent_order=entry_order)
