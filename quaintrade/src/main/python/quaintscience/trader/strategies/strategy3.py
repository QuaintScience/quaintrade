from __future__ import annotations
from typing import Optional
import datetime
from enum import Enum
import pandas as pd
import numpy as np
from ..core.ds import (Order,
                       TradeType,
                       OrderState,
                       OrderType,
                       TransactionType,
                       PositionType,
                       TradingProduct)

from ..core.strategy import Strategy
from ..core.indicator import (IndicatorPipeline,
                              HeikinAshiIndicator,
                              WMAIndicator,
                              PivotIndicator,
                              ATRIndicator,
                              ADXIndicator,
                              RSIIndicator,
                              GapUpDownIndicator,
                              DonchianIndicator)
from ..core.ml.lorentzian import LorentzianClassificationIndicator
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





class Strategy3(Strategy):

    def __init__(self,
                *args,
                atr_period: int = 14,
                wma_period: int = 50,
                product: TradingProduct = TradingProduct.MIS,
                **kwargs):
        self.product = product
        self.atr_period = atr_period
        self.wma_period = wma_period
        self.atr_col = f"ATR_{self.atr_period}"
        self.wma_col = f"WMA_{self.wma_period}"
        indicators =IndicatorPipeline([(ATRIndicator(period=self.atr_period), None, None),
                                       (WMAIndicator(period=self.wma_period), None, None),
                                       (LorentzianClassificationIndicator(neighbors_count = 16,
                                                                          user_ema_filter=False,
                                                                          use_sma_filter=False,
                                                                          use_adx_filter=False,
                                                                          use_kernel_smoothing=True,
                                                                          use_dynamic_exists=True,
                                                                          use_volatility_filter=True,
                                                                          regime_threshold=-0.1), None, None)])

        kwargs["indicator_pipeline"] = {"window": indicators,
                                        "context": {}}
        kwargs["plottables"] = {"indicator_fields": [{"field": "yhat1", "color": "red", "panel": 0},
                                                     {"field": self.wma_col, "color": "black", "panel": 0},
                                                     {"field": "yhat2", "color": "green", "panel": 0},
                                                     {"field": "isBullish", "color": "green", "panel": 2},
                                                     {"field": "isBearish", "color": "red", "panel": 2}]}
        kwargs["plot_context_candles"] = []
        non_trading_timeslots = []
        #non_trading_timeslots.extend(Strategy.NON_TRADING_FIRST_HOUR)
        #non_trading_timeslots.extend([{"from": {"hour": 9,
        #                               "minute": 15},
        #                               "to": {"hour": 9,
        #                               "minute": 25}}])
        kwargs["non_trading_timeslots"] = non_trading_timeslots
        kwargs["context_required"] = []
    
        kwargs["intraday_squareoff"] = False


        self.entry_atr_factor = 0.02
        self.sl_atr_factor = 3.0
        self.risk_reward_ratio = 10.0
        super().__init__(*args, **kwargs)


    def apply_impl(self,
                broker: Broker,
                scrip: str,
                exchange: str,
                window: pd.DataFrame,
                context: dict[str, pd.DataFrame]) -> Optional[TradeType]:

        if self.can_trade(window, context):

            
            
            is_bullish = all(window.iloc[-1:]["isBullish"]) and not window.iloc[-2]["isBullish"]
            is_bearish = window.iloc[-1]["isBearish"] #and not window.iloc[-2]["isBearish"]
            
            next_run = None
        
            if (is_bullish):
                next_run = TradeType.LONG
            if is_bearish:
                self.perform_squareoff(broker=broker,
                                        scrip=scrip,
                                        exchange=exchange,
                                        product=self.product)

            if next_run is not None:

                qty = max(self.max_budget // window.iloc[-1]["close"],
                          self.min_quantity)

                broker.place_express_order(scrip=scrip,
                                           exchange=exchange,
                                           quantity=qty,
                                           transaction_type=TransactionType.BUY,
                                           order_type=OrderType.MARKET,
                                           tags=["entry",
                                                 self.long_position_tag],
                                           strategy=self.strategy_name,
                                           run_name=broker.run_name,
                                           run_id=broker.run_id)
