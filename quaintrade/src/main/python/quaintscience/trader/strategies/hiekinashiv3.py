from typing import Optional

import pandas as pd

from ..core.ds import (TradeType, OrderState, PositionType)

from ..core.strategy import Strategy
from ..core.indicator import (IndicatorPipeline,
                              HeikinAshiIndicator,
                              SupertrendIndicator,
                              SMAIndicator,
                              SlopeIndicator,
                              DonchainIndicator)
from ..core.roles import Broker

# Not in use
class HiekinAshiStrategy(Strategy):

    def __init__(self,
                 *args,
                 st_period: int = 7,
                 st_multiplier: float = 2.5,
                 ma_period: int = 33,
                 donchain_period: int = 15,
                 **kwargs):
        self.st_period = st_period
        self.st_multiplier = st_multiplier
        self.ma_period = ma_period
        self.donchain_period = donchain_period
        indicators = indicators=[(HeikinAshiIndicator(), None, None),
                                 #(SupertrendIndicator(period=self.st_period,
                                 #                     multiplier=self.st_multiplier), None, None),
                                 (SMAIndicator(period=self.ma_period), None, None),
                                 #(SlopeIndicator(signal=f"SMA_{self.ma_period}"), None, None),
                                 #(DonchainIndicator(period=self.donchain_period), None, None)
                                 ]
        kwargs["indicator_pipeline"] = IndicatorPipeline(indicators=indicators)
        kwargs["plottables"] = {"indicator_fields": [{"field": "ha_long_trend", "panel": 2},
                                                     {"field": "ha_short_trend", "panel": 3},
                                                     {"field": "ha_non_trending", "panel": 4},
                                                     f"SMA_{self.ma_period}",
                                                     #f"supertrend_{self.st_period}_{self.st_multiplier:.1f}",
                                                     #f"SMA_{self.ma_period}",
                                                     #f"donchainUpper_{self.donchain_period}",
                                                     #f"donchainLower_{self.donchain_period}",
                                                     ]}
        non_trading_timeslots = []
        #non_trading_timeslots.extend(Strategy.NON_TRADING_FIRST_HOUR)
        #non_trading_timeslots.extend(Strategy.NON_TRADING_AFTERNOON)
        kwargs["non_trading_timeslots"] = non_trading_timeslots
        kwargs["context_required"] = ["2h", "1h"]
        kwargs["intraday_squareoff"] = False
        self.current_run = None

        self.target_amt = 400

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

    def cancel_active_orders(self, broker: Broker):
        for order in broker.get_orders():
            if (order.state == OrderState.PENDING
                and ("hiekinashi_long" in order.tags
                     or "hiekinashi_short" in order.tags)):
                broker.cancel_order(order)
        self.perform_squareoff(broker=broker)

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
        self.logger.info(f"Current Run: {self.current_run}")
        #if self.current_run == TradeType.LONG and window.iloc[-1]["ha_short_trend"] == 1.0:
        #    self.cancel_active_orders(broker=broker)
        #    self.current_run = None
        #elif self.current_run == TradeType.SHORT and window.iloc[-1]["ha_long_trend"] == 1.0:
        #    self.cancel_active_orders(broker=broker)
        #    self.current_run = None
            
        if self.can_trade(window, context):
            make_entry = False
            self.logger.info(f'Trigger from current Interval: {window.iloc[-1]["ha_long_trend"] == 1.0 and window.iloc[-2]["ha_long_trend"] != 1.0}')
            self.logger.info(f'2h context status: Long-trend: {context["2h"].iloc[-1]["ha_long_trend"]} | Short-trend {context["2h"].iloc[-1]["ha_short_trend"]}')
            self.logger.info(f'1h context status: Long-trend: {context["1h"].iloc[-1]["ha_long_trend"]} | Short-trend {context["1h"].iloc[-1]["ha_short_trend"]}')
            if (window.iloc[-1]["ha_long_trend"] == 1.0 and window.iloc[-2]["ha_long_trend"] != 1.0
                #and context["2h"].iloc[-1]["ha_trending_green"] == 1.0
                #and context["1h"].iloc[-1]["ha_trending_green"] == 1.0
                and self.current_run != TradeType.LONG):
                self.current_run = TradeType.LONG
                make_entry = True
                self.logger.debug(f"Entering long trade!")
            elif (window.iloc[-1]["ha_short_trend"] == 1.0 and window.iloc[-2]["ha_short_trend"] != 1.0
                #and context["2h"].iloc[-1]["ha_trending_red"] == 1.0
                #and context["1h"].iloc[-1]["ha_trending_red"] == 1.0
                and self.current_run != TradeType.SHORT):
                self.current_run = TradeType.SHORT
                make_entry = True
                self.logger.debug(f"Entering short trade!")
            if make_entry:
                self.logger.debug(f"Taking position!")
                self.cancel_active_orders(broker=broker)
                self.take_position(scrip=scrip,
                                   exchange=exchange,
                                   broker=broker,
                                   position_type=PositionType.ENTRY,
                                   trade_type=self.current_run,
                                   price=self.get_entry(window, self.current_run),
                                   quantity=100,
                                   tags=[f"hiekinashi_{self.current_run.value}"])
