from abc import ABC, abstractmethod
from typing import Optional, Union
import copy

import pandas as pd

from .logging import LoggerMixin
from .ds import Order, TradeType, OrderType, OrderState
from .indicator import Indicator

from ..integration.common import TradeManager



class StrategyExecutor(ABC, LoggerMixin):

    NON_TRADING_FIRST_HOUR = [{"from": {"hour": 9, "minute": 0},
                               "to": {"hour" 9, "minute": 59}}]
    NON_TRADING_AFTERNOON = [{"from": {"hour": 2, "minute": 30},
                              "to": {"hour" 3, "minute": 15}}]

    def __init__(self,
                 indicators: dict[str, Indicator],
                 order_indicator: str,
                 buy_order_template: Order,
                 sell_order_template: Order,
                 trade_manager: TradeManager,
                 non_trading_timeslots: list[dict[str, str]] = None,
                 intraday_squareoff: bool = True,
                 squareoff_hour: int = 15,
                 squareoff_minute: int = 10,
                 *args, **kwargs):
        self.indicators = indicators
        self.order_indicator = order_indicator
        self.buy_order_template = buy_order_template
        self.sell_order_template = sell_order_template
        self.intraday_squareoff = intraday_squareoff
        self.squareoff_hour = squareoff_hour
        self.squareoff_minute = squareoff_minute
        if non_trading_timeslots is None:
            non_trading_timeslots = []
        self.non_trading_timeslots = non_trading_timeslots
        self.trade_manager = trade_manager
        super().__init__(*args, **kwargs)

    def perform_squareoff(self, df: pd.DataFrame, idx: int):
        if (df.iloc[idx].index.dt.hour >= self.squareoff_hour and
            df.iloc[idx].index.dt.minute >= self.squareoff_minute):
            positions = self.trade_manager.get_positions()
            for position in positions:
                if position.quantity > 0:
                    self.trade_manager.express_market_buy_order(scrip=position.scrip,
                                                                exchange=position.exchange,
                                                                quantity=position.quantity,
                                                                trade_type=TradeType.SELL)
                if position.quantity < 0:
                    self.trade_manager.express_market_buy_order(scrip=position.scrip,
                                                                exchange=position.exchange,
                                                                quantity=-position.quantity,
                                                                trade_type=TradeType.BUY)

    def can_trade(self, df: pd.DataFrame, idx: int):
        row = df.iloc[idx]
        for non_trading_timeslot in self.non_trading_timeslots:
            if (row.index.dt.hour > non_trading_timeslot["from"]["hour"]
                or (row.index.dt.hour == non_trading_timeslot["from"]["hour"] and
                    row.index.dt.minute >= non_trading_timeslot["from"]["minute"])):
                if (row.index.dt.hour < non_trading_timeslot["to"]["hour"]
                    or (row.index.dt.hour == non_trading_timeslot["to"]["hour"] and
                    row.index.dt.minute <= non_trading_timeslot["to"]["minute"])):
                    return False
        return True

    def go_long(self):
        entry_order = copy.deepcopy(self.buy_order_template)
        entry_order.order_type = OrderType.LIMIT
        entry_order.limit_price = self.get_entry_price()
        self.trade_manager.place_order()

    @abstractmethod
    def compute(self, df: pd.DataFrame,
                output_column_name: Optional[str] = None,
                stream: bool = False) -> Union[pd.DataFrame, Order]:
        pass

    @property
    def pending_orders(self) -> list[Order]:
        return [order for order in self.trade_manager.get_orders() if order.state == OrderState.PENDING]


class NextPsychologicalPriceEntryMixin():

    def __init__(self,
                 price_column: str,
                 *args,
                 psychological_number: float = 10,
                 **kwargs):
        self.psychological_number = psychological_number
        self.price_column = price_column

    def get_entry_price(self, window: pd.DataFrame, trade_type: TradeType):
        if trade_type == TradeType.BUY:
            window.iloc[-1]

class AbsoluteStopLossStrategyMixin():

    def __init__(self,
                 absolute_stoploss_value: float,
                 absolute_target_value: float,
                 *args,
                 **kwargs):
        self.absolute_stoploss_value = absolute_stoploss_value
        self.absolute_target_value = absolute_target_value
        super().__init__(*args, **kwargs)
    
    def get_stoploss(self, window: pd.DataFrame, trade_type: TradeType) -> float:
        return self.absolute_stoploss_value

    def get_target(self, window: pd.DataFrame, trade_type: TradeType) -> float:
        return self.absolute_target_value


class RelativeStopLossStrategyExecutorMixin():

    def __init__(self,
                 relative_stoploss_value: float,
                 relative_target_value: float,
                 price_column: str,
                 *args,
                 **kwargs):
        self.relative_stoploss_value = relative_stoploss_value
        self.relative_target_value = relative_target_value
        self.price_column = price_column
        super().__init__(*args, **kwargs)
    
    def get_stoploss(self, window: pd.DataFrame, trade_type: TradeType) -> float:
        if trade_type == TradeType.BUY:
            return window.iloc[-1][self.price_column] - self.relative_stoploss_value
        else:
            return window.iloc[-1][self.price_column] + self.relative_stoploss_value
    
    def get_target(self, window: pd.DataFrame, trade_type: TradeType) -> float:
        if trade_type == TradeType.BUY:
            return window.iloc[-1][self.price_column] + self.relative_target_value
        else:
            return window.iloc[-1][self.price_column] - self.relative_target_value


class CandleStopLossStrategyExecutorMixin():

    def __init__(self,
                 entry_column: str,
                 stoploss_column: str,
                 *args,
                 entry_candle_idx: int = -1,
                 stoploss_candle_idx: int = -1,
                 extra: float = 1.0,
                 target_multiplier: float = 2.0,
                 **kwargs):
        self.entry_column = entry_column
        self.stoploss_column = stoploss_column
        self.extra = extra
        self.target_multiplier = target_multiplier
        self.entry_candle_idx = entry_candle_idx
        self.stoploss_candle_idx = stoploss_candle_idx
        super().__init__(*args, **kwargs)
    
    def get_stoploss(self, window: pd.DataFrame, trade_type: TradeType) -> float:
        if trade_type == TradeType.BUY:
            return window.iloc[self.stoploss_candle_idx][self.stoploss_column] - self.extra
        else:
            return window.iloc[self.stoploss_candle_idx][self.stoploss_column] + self.extra
    
    def get_target(self, window: pd.DataFrame, trade_type: TradeType) -> float:
        abs_val = self.target_multiplier * abs(window.iloc[self.entry_candle_idx][self.entry_column] - window.iloc[self.stoploss_candle_idx][self.stoploss_column])
        if trade_type == TradeType.BUY:
            return window.iloc[self.entry_candle_idx][self.entry_column] + abs_val
        else:
            return window.iloc[self.entry_candle_idx][self.entry_column] - abs_val


class IndicatorValueBasedStopLossStrategyExecutorMixin():

    def __init__(self,
                 absolute_target_value: float,
                 stoploss_indicator_column: str,
                 price_column: str,
                 *args,
                 extra: float = 1.0,
                 **kwargs):

        self.absolute_target_value = absolute_target_value
        self.stoploss_indicator_column = stoploss_indicator_column
        self.price_column = price_column
        self.extra = extra
        super().__init__(*args, **kwargs)
    
    def get_stoploss(self, window: pd.DataFrame, trade_type: TradeType) -> float:
        return window.iloc[-1][self.stoploss_indicator_column]
    
    def get_target(self, window: pd.DataFrame, trade_type: TradeType) -> float:
        return self.absolute_target_value
