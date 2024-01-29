from typing import Optional

import pandas as pd

from ..core.ds import (TradeType, OrderState, PositionType)

from ..core.strategy import Strategy
from ..core.indicator import (IndicatorPipeline,
                              HeikinAshiIndicator,
                              ATRIndicator,
                              IntradayHighLowIndicator)
from ..core.roles import Broker
from ..core.util import new_id


class PDHBreakout(Strategy):

    def __init__(self,
                 *args,
                 start_hour: int = 10,
                 start_minute: int = 30,
                 end_hour: int = 19,
                 end_minute: int = 0,
                 atr_period: int = 14,
                 **kwargs):
        self.start_hour = start_hour
        self.start_minute = start_minute
        self.end_hour = end_hour
        self.end_minute = end_minute
        self.atr_period = atr_period
        indicators = indicators=[(IntradayHighLowIndicator(start_hour=self.start_hour,
                                                           start_minute=self.start_minute,
                                                           end_hour=self.end_hour,
                                                           end_minute=self.end_minute), None, None),
                                 (ATRIndicator(period=self.atr_period), None, None),
                                 ]
        kwargs["indicator_pipeline"] = IndicatorPipeline(indicators=indicators)
        self.high_col = f"period_high_{self.start_hour}_{self.start_minute}_{self.end_hour}_{self.end_minute}"
        self.low_col = f"period_low_{self.start_hour}_{self.start_minute}_{self.end_hour}_{self.end_minute}"
        kwargs["plottables"] = {"indicator_fields": [self.high_col,
                                                     self.low_col,
                                                     {"field": f"ATR_{self.atr_period}", "panel": 3}
                                                     ]}
        non_trading_timeslots = []
        non_trading_timeslots.extend([{"from": {"hour": 22,
                                        "minute": 30},
                                        "to": {"hour": 23,
                                        "minute": 59}}])
        #non_trading_timeslots.extend(Strategy.NON_TRADING_AFTERNOON)
        kwargs["non_trading_timeslots"] = non_trading_timeslots
        kwargs["context_required"] = ["1h"]
        kwargs["squareoff_hour"] = 23
        kwargs["squareoff_minute"] = 00

        self.target_amt = 100
        self.sl_amt = 25

        self.entry_threshold = 25

        """
        self.sl_factor = 2
        self.target_factor = 2.5
        """

        self.sl_factor = 0.5
        self.target_factor = 3

        """
        self.sl_factor = 3
        self.target_factor = 3
        """

        print(args, kwargs)
        super().__init__(*args, **kwargs)

    def get_entry(self, window: pd.DataFrame, trade_type: TradeType):
        if trade_type == TradeType.LONG:
            return window.iloc[-1][self.high_col] + self.entry_threshold
        else:
            return window.iloc[-1][self.low_col] - self.entry_threshold

    def get_target(self, window: pd.DataFrame, context: dict[str, pd.DataFrame], trade_type: TradeType):
        """
        if trade_type == TradeType.LONG:
            return window.iloc[-1]["high"] + self.risk_ratio * abs(self.get_entry(window, trade_type) - self.get_stoploss(window, trade_type))
        else:
            return window.iloc[-1]["low"] - self.risk_ratio * abs(self.get_stoploss(window, trade_type) - self.get_entry(window, trade_type))
        """
        """
        if trade_type == TradeType.LONG:
            return window.iloc[-1][self.high_col] + self.target_factor * window.iloc[-1][f"ATR_{self.atr_period}"]
        else:
            return window.iloc[-1][self.low_col] - self.target_factor * window.iloc[-1][f"ATR_{self.atr_period}"]
        """
        if trade_type == TradeType.LONG:
            return window.iloc[-1][self.high_col] + self.target_amt
        else:
            return window.iloc[-1][self.low_col] - self.target_amt
    def get_stoploss(self, window: pd.DataFrame, context: dict[str, pd.DataFrame], trade_type: TradeType):
        """
        if trade_type == TradeType.LONG:
            return window.iloc[-1][self.high_col] - self.sl_factor * window.iloc[-1][f"ATR_{self.atr_period}"]
        else:
            return window.iloc[-1][self.high_col] + self.sl_factor * window.iloc[-1][f"ATR_{self.atr_period}"]
        """
        if trade_type == TradeType.LONG:
            return window.iloc[-1][self.high_col] - self.sl_amt
        else:
            return window.iloc[-1][self.high_col] + self.sl_amt

    def get_current_run(self, broker: Broker):
        for order in broker.get_orders():
            if (order.state == OrderState.PENDING
                and "afternoon_breakout_long" in order.tags):
                return TradeType.LONG
            elif (order.state == OrderState.PENDING
                and "afternoon_breakout_short" in order.tags):
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

        if (((window.iloc[-1].name.hour == self.end_hour
            and window.iloc[-1].name.minute > self.end_minute)
            or (window.iloc[-1].name.hour > self.end_hour))
            and self.get_current_run(broker) is None
            and self.can_trade(window, context)):
            qty = max(self.max_budget // window.iloc[-1]["close"], self.min_quantity)
            
            group_id = new_id()
            if context["1h"].iloc[-1]["ha_trending_green"] == 1.0:
                self.logger.debug(f"Taking LONG position!")
                entry_order = self.take_position(scrip=scrip,
                                                    exchange=exchange,
                                                    broker=broker,
                                                    position_type=PositionType.ENTRY,
                                                    trade_type=TradeType.LONG,
                                                    price=self.get_entry(window, TradeType.LONG),
                                                    quantity=qty,
                                                    tags=[f"afternoon_breakout_long"],
                                                    group_id=group_id)
                self.take_position(scrip=scrip,
                                    exchange=exchange,
                                    broker=broker,
                                    position_type=PositionType.STOPLOSS,
                                    trade_type=TradeType.LONG,
                                    price=self.get_stoploss(window, context, TradeType.LONG),
                                    quantity=qty,
                                    tags=[f"afternoon_breakout_long"],
                                    parent_order=entry_order,
                                    group_id=group_id)
                self.take_position(scrip=scrip,
                                    exchange=exchange,
                                    broker=broker,
                                    position_type=PositionType.TARGET,
                                    trade_type=TradeType.LONG,
                                    price=self.get_target(window, context, TradeType.LONG),
                                    quantity=qty,
                                    tags=[f"afternoon_breakout_long"],
                                    parent_order=entry_order,
                                    group_id=group_id)
            if context["1h"].iloc[-1]["ha_trending_red"] == 1.0:
                self.logger.debug(f"Taking SHORT position!")
                entry_order = self.take_position(scrip=scrip,
                                                    exchange=exchange,
                                                    broker=broker,
                                                    position_type=PositionType.ENTRY,
                                                    trade_type=TradeType.SHORT,
                                                    price=self.get_entry(window, TradeType.SHORT),
                                                    quantity=qty,
                                                    tags=[f"afternoon_breakout_short"],
                                                    group_id=group_id)
                self.take_position(scrip=scrip,
                                    exchange=exchange,
                                    broker=broker,
                                    position_type=PositionType.STOPLOSS,
                                    trade_type=TradeType.SHORT,
                                    price=self.get_stoploss(window, context, TradeType.SHORT),
                                    quantity=qty,
                                    tags=[f"afternoon_breakout_short"],
                                    parent_order=entry_order,
                                    group_id=group_id)
                self.take_position(scrip=scrip,
                                    exchange=exchange,
                                    broker=broker,
                                    position_type=PositionType.TARGET,
                                    trade_type=TradeType.SHORT,
                                    price=self.get_target(window, context, TradeType.SHORT),
                                    quantity=qty,
                                    tags=[f"afternoon_breakout_short"],
                                    parent_order=entry_order,
                                    group_id=group_id)
