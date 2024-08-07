from typing import Optional

import pandas as pd
import numpy as np

from ..core.ds import (TradeType, OrderState, PositionType)

from ..core.strategy import Strategy
from ..core.indicator import (IndicatorPipeline,
                              HeikinAshiIndicator,
                              BBANDSIndicator,
                              ATRIndicator,
                              SMAIndicator,
                              SlopeIndicator,
                              DonchainIndicator)
from ..core.roles import Broker


class DonchainBreakout(Strategy):

    def __init__(self,
                 *args,
                 donchain_period: int = 15,
                 atr_period: int = 15,
                 **kwargs):
        self.st_period = donchain_period
        self.atr_period = atr_period
        indicators = indicators=[(HeikinAshiIndicator(), None, None),
                                 (DonchainIndicator(period=self.donchain_period), None, None),
                                 (ATRIndicator(period=self.atr_period), None, None),
                                 ]
        kwargs["indicator_pipeline"] = IndicatorPipeline(indicators=indicators)
        kwargs["plottables"] = {"indicator_fields": [{"field": "ha_long_trend", "panel": 2},
                                                     {"field": "ha_trending_green", "panel": 2, "context": "1h"},
                                                     {"field": "ha_short_trend", "panel": 3},
                                                     {"field": "ha_trending_red", "panel": 3, "context": "1h"},
                                                     #{"field": "ha_non_trending", "panel": 4},
                                                     f"DonchainUpper_{self.donchain_period}",
                                                     f"DonchainLower_{self.donchain_period}",
                                                     {"field": f"ATR_{self.atr_period}", "panel": 5},
                                                     #f"supertrend_{self.st_period}_{self.st_multiplier:.1f}",
                                                     #f"SMA_{self.ma_period}",
                                                     #f"donchainUpper_{self.donchain_period}",
                                                     #f"donchainLower_{self.donchain_period}",
                                                     ]}
        non_trading_timeslots = []
        non_trading_timeslots.extend(Strategy.NON_TRADING_FIRST_HOUR)
        non_trading_timeslots.extend(Strategy.NON_TRADING_AFTERNOON)
        kwargs["non_trading_timeslots"] = non_trading_timeslots
        kwargs["context_required"] = ["1h"]

        self.target_amt = 3

        self.entry_threshold = 0.1

        """
        self.sl_factor = 2
        self.target_factor = 2.5
        """

        self.sl_factor = 2
        self.target_factor = 5

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
            return window.iloc[-1]["high"] + self.entry_threshold
        else:
            return window.iloc[-1]["low"] - self.entry_threshold

    def get_target(self, window: pd.DataFrame, context: dict[str, pd.DataFrame], trade_type: TradeType):
        """
        if trade_type == TradeType.LONG:
            return window.iloc[-1]["high"] + self.risk_ratio * abs(self.get_entry(window, trade_type) - self.get_stoploss(window, trade_type))
        else:
            return window.iloc[-1]["low"] - self.risk_ratio * abs(self.get_stoploss(window, trade_type) - self.get_entry(window, trade_type))
        """

        if trade_type == TradeType.LONG:
            return window.iloc[-1]["high"] + self.target_factor * window.iloc[-1][f"ATR_{self.atr_period}"]
        else:
            return window.iloc[-1]["low"] - self.target_factor * window.iloc[-1][f"ATR_{self.atr_period}"]

    def get_stoploss(self, window: pd.DataFrame, context: dict[str, pd.DataFrame], trade_type: TradeType):
        
        if trade_type == TradeType.LONG:
            return window.iloc[-1]["low"] - self.sl_factor * window.iloc[-1][f"ATR_{self.atr_period}"]
        else:
            return window.iloc[-1]["high"] + self.sl_factor * window.iloc[-1][f"ATR_{self.atr_period}"]

    def cancel_active_orders(self, broker: Broker):
        for order in broker.get_orders():
            if (order.state == OrderState.PENDING
                and ("hiekinashi_long" in order.tags
                     or "hiekinashi_short" in order.tags)):
                broker.cancel_order(order)
                storage = broker.get_tradebook_storage()
                storage.store_order_execution(strategy=self.strategy_name,
                                              run_name=broker.run_name,
                                              run_id=broker.run_id,
                                              date=broker.current_datetime(),
                                              order=order,
                                              event="OrderCancelled")

        self.perform_squareoff(broker=broker)

    def get_current_run(self, broker: Broker):
        for order in broker.get_orders():
            if (order.state == OrderState.PENDING
                and "hiekinashi_long" in order.tags):
                return TradeType.LONG
            elif (order.state == OrderState.PENDING
                and "hiekinashi_short" in order.tags):
                return TradeType.SHORT

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
        current_run = self.get_current_run(broker)
        self.logger.info(f"Current Run: {current_run}")
        if self.can_trade(window, context):
            make_entry = False
            if (((window.iloc[-1]["ha_long_trend"] == 1.0 and window.iloc[-2]["ha_long_trend"] != 1.0)
                 or (window.iloc[-1]["ha_long_trend"] == 1.0 and current_run is None))
                #and context["1d"].iloc[-1]["ha_trending_green"] == 1.0
                and context["1h"].iloc[-1]["ha_trending_green"] == 1.0
                and current_run != TradeType.LONG):
                current_run = TradeType.LONG
                make_entry = True
                self.logger.debug(f"Entering long trade!")
            if ((window.iloc[-1]["ha_short_trend"] == 1.0 and window.iloc[-2]["ha_short_trend"] != 1.0
                 or (window.iloc[-1]["ha_short_trend"] == 1.0 and current_run is None))
                #and context["1d"].iloc[-1]["ha_trending_red"] == 1.0
                and context["1h"].iloc[-1]["ha_trending_red"] == 1.0
                and current_run != TradeType.SHORT):
                current_run = TradeType.SHORT
                make_entry = True
                self.logger.debug(f"Entering short trade!")

            if make_entry and not (np.isnan(self.get_entry(window, current_run))
                                   or np.isnan(self.get_stoploss(window, context, current_run))
                                   or np.isnan(self.get_target(window, context, current_run))):
                qty = max(self.max_budget // window.iloc[-1]["close"], self.min_quantity)
                self.logger.debug(f"Taking position!")
                self.cancel_active_orders(broker=broker)
                entry_order = self.take_position(scrip=scrip,
                                                 exchange=exchange,
                                                 broker=broker,
                                                 position_type=PositionType.ENTRY,
                                                 trade_type=current_run,
                                                 price=self.get_entry(window, current_run),
                                                 quantity=qty,
                                                 tags=[f"hiekinashi_{current_run.value}"])
                self.take_position(scrip=scrip,
                                   exchange=exchange,
                                   broker=broker,
                                   position_type=PositionType.STOPLOSS,
                                   trade_type=current_run,
                                   price=self.get_stoploss(window, context, current_run),
                                   quantity=qty,
                                   tags=[f"hiekinashi_{current_run.value}"],
                                   parent_order=entry_order)
                self.take_position(scrip=scrip,
                                   exchange=exchange,
                                   broker=broker,
                                   position_type=PositionType.TARGET,
                                   trade_type=current_run,
                                   price=self.get_target(window, context, current_run),
                                   quantity=qty,
                                   tags=[f"hiekinashi_{current_run.value}"],
                                   parent_order=entry_order)
