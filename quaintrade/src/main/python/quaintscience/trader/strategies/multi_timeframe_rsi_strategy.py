from typing import Optional

import pandas as pd

from ..core.ds import (TradeType, OrderState, PositionType)

from ..core.strategy import Strategy
from ..core.indicator import (IndicatorPipeline,
                              ATRIndicator,
                              RSIIndicator)
from ..core.roles import Broker
from ..core.util import new_id


class RSIStrategy(Strategy):

    def __init__(self,
                 *args,
                 rsi_period: int = 14,
                 atr_period: int = 14,
                 **kwargs):
        self.rsi_period = rsi_period
        self.atr_period = atr_period
        self.long_context = "1d"
        indicators = indicators=[(RSIIndicator(period=self.rsi_period), None, None),
                                 (ATRIndicator(period=self.atr_period), None, None)]
        kwargs["indicator_pipeline"] = IndicatorPipeline(indicators=indicators)
        kwargs["plottables"] = {"indicator_fields": [{"field": f"RSI_{self.rsi_period}", "panel": 3},
                                                     {"field": f"ATR_{self.atr_period}", "panel": 4},
                                                     {"field": f"RSI_{self.rsi_period}", "panel": 3, "context": self.long_context}
                                                     ]}
        non_trading_timeslots = []
        #non_trading_timeslots.extend(Strategy.NON_TRADING_FIRST_HOUR)
        #non_trading_timeslots.extend(Strategy.NON_TRADING_AFTERNOON)
        #kwargs["non_trading_timeslots"] = non_trading_timeslots
        kwargs["context_required"] = [self.long_context]
        #kwargs["squareoff_hour"] = 15
        #kwargs["squareoff_minute"] = 0
        kwargs["intraday_squareoff"] = False
        self.rsi_col = f"RSI_{self.rsi_period}"

        self.atr_col = f"ATR_{self.atr_period}"
        self.atr_factor = 0.5
        self.sl_atr_factor = 1

        self.rsi_upper_threshold = 50
        self.rsi_lower_threshold = 35

        self.risk_ratio = 1.5

        """
        self.sl_factor = 3
        self.target_factor = 3
        """

        print(args, kwargs)
        super().__init__(*args, **kwargs)

    def get_entry(self, window: pd.DataFrame, context,  trade_type: TradeType):
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

    def get_current_run(self, broker: Broker):
        for order in broker.get_orders():
            if (order.state == OrderState.PENDING
                and "rsi_swing_long" in order.tags):
                return TradeType.LONG
            elif (order.state == OrderState.PENDING
                and "rsi_swing_short" in order.tags):
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

     
        if self.can_trade(window, context) and self.get_current_run(broker) is None:
            
            qty = max(self.max_budget // window.iloc[-1]["close"], self.min_quantity)
            group_id = new_id()
            print(context[self.long_context].iloc[-1][self.rsi_col],
                  window.iloc[-2][self.rsi_col].min(),
                  window.iloc[-1][self.rsi_col])
            if (context[self.long_context].iloc[-1][self.rsi_col] >= self.rsi_upper_threshold
                and (window.iloc[-2][self.rsi_col].min() > self.rsi_lower_threshold
                     and window.iloc[-1][self.rsi_col] >= self.rsi_lower_threshold)
                and window.iloc[-1]["close"] > window.iloc[-1]["open"]):
                self.logger.debug(f"Taking LONG position!")
                entry_order = self.take_position(scrip=scrip,
                                                exchange=exchange,
                                                broker=broker,
                                                position_type=PositionType.ENTRY,
                                                trade_type=TradeType.LONG,
                                                price=self.get_entry(window, context, TradeType.LONG),
                                                quantity=qty,
                                                tags=[f"rsi_swing_long"],
                                                group_id=group_id)
                self.take_position(scrip=scrip,
                                    exchange=exchange,
                                    broker=broker,
                                    position_type=PositionType.STOPLOSS,
                                    trade_type=TradeType.LONG,
                                    price=self.get_stoploss(window, context, TradeType.LONG),
                                    quantity=qty,
                                    tags=[f"rsi_swing_long"],
                                    parent_order=entry_order,
                                    group_id=group_id)
                self.take_position(scrip=scrip,
                                    exchange=exchange,
                                    broker=broker,
                                    position_type=PositionType.TARGET,
                                    trade_type=TradeType.LONG,
                                    price=self.get_target(window, context, TradeType.LONG),
                                    quantity=qty,
                                    tags=[f"rsi_swing_long"],
                                    parent_order=entry_order,
                                    group_id=group_id)

            """
            if (context["10min"].iloc[-1][self.rsi_col] <= self.rsi_upper_threshold
                and window.iloc[-1][self.rsi_col] >= self.rsi_lower_threshold
                and window.iloc[-1]["close"] < window.iloc[-1]["open"]):

                self.logger.debug(f"Taking SHORT position!")
                entry_order = self.take_position(scrip=scrip,
                                                    exchange=exchange,
                                                    broker=broker,
                                                    position_type=PositionType.ENTRY,
                                                    trade_type=TradeType.SHORT,
                                                    price=self.get_entry(window, context, TradeType.SHORT),
                                                    quantity=qty,
                                                    tags=[f"rsi_swing_short"],
                                                    group_id=group_id)
                self.take_position(scrip=scrip,
                                    exchange=exchange,
                                    broker=broker,
                                    position_type=PositionType.STOPLOSS,
                                    trade_type=TradeType.SHORT,
                                    price=self.get_stoploss(window, context, TradeType.SHORT),
                                    quantity=qty,
                                    tags=[f"rsi_swing_short"],
                                    parent_order=entry_order,
                                    group_id=group_id)
                self.take_position(scrip=scrip,
                                    exchange=exchange,
                                    broker=broker,
                                    position_type=PositionType.TARGET,
                                    trade_type=TradeType.SHORT,
                                    price=self.get_target(window, context, TradeType.SHORT),
                                    quantity=qty,
                                    tags=[f"rsi_swing_short"],
                                    parent_order=entry_order,
                                    group_id=group_id)
            """