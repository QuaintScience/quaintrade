from typing import Optional

import pandas as pd
import numpy as np

from ..core.ds import (TradeType, PositionType, TradingProduct)

from ..core.strategy import Strategy
from ..core.indicator import (IndicatorPipeline,
                              HeikinAshiIndicator,
                              BBANDSIndicator,
                              ATRIndicator,
                              RSIIndicator,
                              IchimokuIndicator,
                              SMAIndicator,
                              SlopeIndicator,
                              DonchainIndicator)
from ..core.roles import Broker


class IchimokuStrategyV1(Strategy):

    def __init__(self,
                 *args,
                 product: TradingProduct = TradingProduct.MIS,
                 st_period: int = 7,
                 st_multiplier: float = 2.5,
                 ma_period: int = 10,
                 donchain_period: int = 15,
                 bb_period: int = 15,
                 atr_period: int = 14,
                 rsi_period: int = 14,
                 **kwargs):
        self.st_period = st_period
        self.st_multiplier = st_multiplier
        self.ma_period = ma_period
        self.bb_period = bb_period
        self.donchain_period = donchain_period
        self.atr_period = atr_period
        self.rsi_period = rsi_period
        self.product = product

        self.long_context = "2h"
        self.long_context2 = "75min"
        self.long_context3 = "45min"
        self.rsi_context = "30min"
        self.rsi_upper_threshold = 60
        self.rsi_lower_threshold = 40
        self.rsi_col = f"RSI_{self.rsi_period}"
        self.atr_col = f"ATR_{self.atr_period}"
        indicators = indicators=[(HeikinAshiIndicator(), None, None),
                                 (IchimokuIndicator(), None, None),
                                 (ATRIndicator(period=self.atr_period), None, None),
                                 ]
        kwargs["indicator_pipeline"] = IndicatorPipeline(indicators=indicators)
        kwargs["plottables"] = {"indicator_fields": [
                                                     "tenkan_sen",
                                                     "kijun_sen",
                                                     "senkou_span_a",
                                                     "senkou_span_b",
                                                     "chikou_span",
                                                     {"field": self.atr_col, "panel": 2},
                                                     ]}
        non_trading_timeslots = []
        non_trading_timeslots.extend(Strategy.NON_TRADING_FIRST_HOUR)
        non_trading_timeslots.extend(Strategy.NON_TRADING_AFTERNOON)
        kwargs["non_trading_timeslots"] = non_trading_timeslots
        kwargs["context_required"] = list(set([self.long_context, self.long_context2,
                                               self.long_context3, self.rsi_context]))

        self.entry_threshold = 0.2
        """
        self.sl_factor = 2
        self.target_factor = 2.5
        """

        self.sl_factor = 2
        self.target_factor = 10

        #self.sl_factor = 10
        #self.target_factor = 10

        """
        self.sl_factor = 3
        self.target_factor = 3
        """

        print(args, kwargs)
        super().__init__(*args, **kwargs)

    def get_entry(self, window: pd.DataFrame, trade_type: TradeType):
        if trade_type == TradeType.LONG:
            return window.iloc[-1]["high"] + self.entry_threshold * window.iloc[-1][self.atr_col]
        else:
            return window.iloc[-1]["low"] - self.entry_threshold * window.iloc[-1][self.atr_col]

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

        current_run = self.get_current_run(broker,
                                           scrip,
                                           exchange)
        self.logger.info(f"Current Run: {current_run}")
        pass