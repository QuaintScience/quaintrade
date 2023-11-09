from abc import ABC, abstractmethod
from typing import Optional, Union
import copy
from functools import partial

import pandas as pd

from .logging import LoggerMixin
from .ds import (Order,
                 TradeType,
                 OrderType,
                 TransactionType,
                 ExecutionType,
                 TradingProduct)
from .indicator import IndicatorPipeline

from ..integration.common import TradeManager
from ..integration.paper import PaperTradeManager


class StrategyExecutor(ABC, LoggerMixin):

    NON_TRADING_FIRST_HOUR = [{"from": {"hour": 9,
                                        "minute": 0},
                               "to": {"hour": 9,
                                      "minute": 59}}]
    NON_TRADING_AFTERNOON = [{"from": {"hour": 2,
                                       "minute": 30},
                              "to": {"hour": 3,
                                     "minute": 15}}]

    def __init__(self,
                 signal_scrip: str,
                 long_scrip: str,
                 short_scrip: str,
                 exchange: str,
                 indicator_pipeline: IndicatorPipeline,
                 *args,
                 default_quantity: int = 1,
                 product: TradingProduct = TradingProduct.MIS,
                 non_trading_timeslots: list[dict[str, str]] = None,
                 intraday_squareoff: bool = True,
                 squareoff_hour: int = 15,
                 squareoff_minute: int = 10,
                 execution_type: ExecutionType = ExecutionType.BACKTESTING,
                 moving_window_size: int = 2,
                 trade_manager: TradeManager = None,
                 **kwargs):
        self.signal_scrip = signal_scrip
        self.long_scrip = long_scrip
        self.short_scrip = short_scrip
        self.product = product
        self.exchange = exchange
        self.indicator_pipeline = indicator_pipeline
        self.default_quantity = default_quantity
        self.intraday_squareoff = intraday_squareoff
        self.squareoff_hour = squareoff_hour
        self.squareoff_minute = squareoff_minute
        if non_trading_timeslots is None:
            non_trading_timeslots = []
        self.non_trading_timeslots = non_trading_timeslots
        if trade_manager is None:
            # If no trade maanger is given, use paper trading...
            instruments = [{"scrip": scrip, "exchange": exchange}
                           for scrip in list(set([signal_scrip, long_scrip, short_scrip]))]
            trade_manager = PaperTradeManager(instruments=instruments,
                                              load_from_files=True,
                                              load_from_redis=True)
            self.trade_manager.start_login()
            self.trade_manager.init()        
        self.trade_manager = trade_manager
        self.execution_type = execution_type
        self.moving_window_size = moving_window_size
        self.order_journal = []

        super().__init__(*args, **kwargs)

    def perform_squareoff(self, window: pd.DataFrame):
        if self.execution_type == ExecutionType.BACKTESTING:
            self.trade_manager.set_backtesting_time(window.iloc[-1].index[0].to_pydatetime())
        if (window.iloc[-1].index.dt.hour >= self.squareoff_hour and
            window.iloc[-1].index.dt.minute >= self.squareoff_minute):
            self.trade_manager.cancel_pending_orders()
            positions = self.trade_manager.get_positions()
            for position in positions:
                if position.quantity > 0:
                    self.trade_manager.place_express_order(scrip=position.scrip,
                                                           exchange=position.exchange,
                                                           quantity=position.quantity,
                                                           transaction_type=TransactionType.SELL,
                                                           order_type=OrderType.MARKET)
                if position.quantity < 0:
                    self.trade_manager.place_express_order(scrip=position.scrip,
                                                           exchange=position.exchange,
                                                           quantity=-position.quantity,
                                                           trade_type=TransactionType.BUY,
                                                           order_type=OrderType.MARKET)

    def can_trade(self, window: pd.DataFrame):
        row = window.iloc[-1]
        for non_trading_timeslot in self.non_trading_timeslots:
            if (row.index.dt.hour > non_trading_timeslot["from"]["hour"]
                or (row.index.dt.hour == non_trading_timeslot["from"]["hour"] and
                    row.index.dt.minute >= non_trading_timeslot["from"]["minute"])):
                if (row.index.dt.hour < non_trading_timeslot["to"]["hour"]
                    or (row.index.dt.hour == non_trading_timeslot["to"]["hour"] and
                    row.index.dt.minute <= non_trading_timeslot["to"]["minute"])):
                    return False
        return True
    
    @abstractmethod
    def get_entry(self, window: pd.DataFrame, trade_type: TradeType):
        pass

    @abstractmethod
    def get_stoploss(self, window: pd.DataFrame, trade_type: TradeType) -> float:
        pass

    @abstractmethod
    def get_target(self, window: pd.DataFrame, trade_type: TradeType) -> float:
        pass

    def get_quantity(self, window: pd.DataFrame, trade_type: TradeType):
        return self.default_quantity

    def take_position(self, window: pd.DataFrame, trade_type: TradeType):
        scrip = None
        if trade_type == TradeType.LONG:
            scrip = self.long_scrip
            transaction_type = TransactionType.BUY
        else:
            scrip = self.short_scrip
            entry_price = self.get_entry(window=window,
                                         trade_type=TradeType.SELL)
            transaction_type = TransactionType.SELL
        order_template = partial(self.trade_manager.create_express_order,
                                 scrip=scrip,
                                 exchange=self.exchange,
                                 quantity=self.get_quantity(window, trade_type),
                                 product=self.product)
        
        entry_order = order_template(transaction_type=transaction_type,
                                     order_type=OrderType.LIMIT,
                                     limit_price=self.get_entry(window=window,
                                                                      trade_type=trade_type))
        entry_order = self.trade_manager.place_order(entry_order)

        stoploss_order = None
        if trade_type == TradeType.LONG:
            transaction_type = TransactionType.SELL
        else:
            transaction_type = TransactionType.BUY
        sl_price = self.get_stoploss(window=window, trade_type=TradeType.BUY)
        stoploss_order = order_template(transaction_type=transaction_type,
                                        order_type=OrderType.SL_LIMIT,
                                        limit_price=sl_price,
                                        trigger_price=sl_price)

        stoploss_order = self.trade_manager.place_another_order_on_entry(entry_order, stoploss_order)
        self.order_journal.append({"entry": entry_order,
                                   "sl": stoploss_order,
                                   "timestamp": window.iloc[-1].index})

    @abstractmethod
    def strategy(self, window: pd.DataFrame) -> Optional[TradeType]:
        pass

    def trade(self,
              df: pd.DataFrame,
              stream: bool = False) -> list[Order]:
        if not stream:
            for ii in range(0, len(df) - self.moving_window_size + 1, 1):
                window = df.iloc[ii: ii + self.moving_window_size]
                if self.execution_type == ExecutionType.BACKTESTING:
                    self.trade_manager.set_current_time(window.iloc[-1].name.to_pydatetime(),
                                                        traverse=True)
                self.trade_manager
                ts = window.iloc[-1].index
                if not self.can_trade(window):
                    self.logger.info(f"Cannot trade at {ts}")
                    continue
                self.logger.debug(f"Trading at {ts}")
                window = self.indicator_pipeline.compute(window)
                trade_type = self.strategy(window)
                if trade_type is None:
                    continue
                self.logger.info(f"Found trade {trade_type} at {ts}")
                self.take_position(window, trade_type)
                self.perform_squareoff(window)
        else:
            if self.can_trade(df):
                trade_type = self.strategy(df)
                if trade_type is not None:
                    self.logger.info(f"Found trade {trade_type} at {ts}")
                    self.take_position(df, trade_type)
            self.perform_squareoff(df)


class PriceEntryMixin(ABC):

    @abstractmethod
    def get_entry(self, window: pd.DataFrame, trade_type: TradeType):
        pass


class NextPsychologicalPriceEntryMixin(PriceEntryMixin):

    def __init__(self,
                 price_column: str,
                 *args,
                 psychological_number: float = 10,
                 extra: float = 1,
                 **kwargs):
        self.psychological_number = psychological_number
        self.price_column = price_column
        self.extra = extra
        super().__init__(*args, **kwargs)

    def get_entry(self, window: pd.DataFrame, trade_type: TradeType):
        x = window.iloc[-1][self.price_column]
        if trade_type == TradeType.BUY:
            return (x - x % self.psychological_number) + self.psychological_number + self.extra
        else:
            return (x - x % self.psychological_number)


class CandleBasedPriceEntryMixin(PriceEntryMixin):

    def __init__(self,
                 entry_price_column: str,
                 *args,
                 idx: int = -1,
                 extra: float = 1,
                 **kwargs):
        self.price_column = entry_price_column
        self.extra = extra
        self.idx = idx
        super().__init__(*args, **kwargs)

    def get_entry(self, window: pd.DataFrame, trade_type: TradeType):
        x = window.iloc[self.idx][self.price_column]
        if trade_type == TradeType.LONG:
            return x + self.extra
        else:
            return x - self.extra



class StopLossAndTargetMixin(ABC):

    def __init__(self, *args, is_trailing: bool = False, **kwargs):
        self.is_trailing = is_trailing
        super().__init__(*args, **kwargs)

    @abstractmethod
    def get_stoploss(self, window: pd.DataFrame, trade_type: TradeType) -> float:
        pass

    @abstractmethod
    def get_target(self, window: pd.DataFrame, trade_type: TradeType) -> float:
        pass


class AbsoluteStopLossAndTargetMixin(StopLossAndTargetMixin):

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


class RelativeStopLossAndTargetMixin(StopLossAndTargetMixin):

    def __init__(self,
                 relative_stoploss_value: float,
                 relative_target_value: float,
                 sl_target_price_column: str,
                 *args,
                 **kwargs):
        self.relative_stoploss_value = relative_stoploss_value
        self.relative_target_value = relative_target_value
        self.price_column = sl_target_price_column
        super().__init__(*args, **kwargs)

    def get_stoploss(self, window: pd.DataFrame, trade_type: TradeType) -> float:
        if trade_type == TradeType.LONG:
            return window.iloc[-1][self.price_column] - self.relative_stoploss_value
        else:
            return window.iloc[-1][self.price_column] + self.relative_stoploss_value
    
    def get_target(self, window: pd.DataFrame, trade_type: TradeType) -> float:
        if trade_type == TradeType.LONG:
            return window.iloc[-1][self.price_column] + self.relative_target_value
        else:
            return window.iloc[-1][self.price_column] - self.relative_target_value


class CandleBasedStopLossAndTargetMixin(StopLossAndTargetMixin):

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
        if trade_type == TradeType.LONG:
            return window.iloc[self.stoploss_candle_idx][self.stoploss_column] - self.extra
        else:
            return window.iloc[self.stoploss_candle_idx][self.stoploss_column] + self.extra
    
    def get_target(self, window: pd.DataFrame, trade_type: TradeType) -> float:
        abs_val = self.target_multiplier * abs(window.iloc[self.entry_candle_idx][self.entry_column] - window.iloc[self.stoploss_candle_idx][self.stoploss_column])
        if trade_type == TradeType.LONG:
            return window.iloc[self.entry_candle_idx][self.entry_column] + abs_val
        else:
            return window.iloc[self.entry_candle_idx][self.entry_column] - abs_val


class IndicatorValueBasedStopLossAndTargetMixin(StopLossAndTargetMixin):

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
        if trade_type == TradeType.LONG:
            return window.iloc[-1][self.stoploss_indicator_column] - self.extra
        else:
            return window.iloc[-1][self.stoploss_indicator_column] + self.extra

    def get_target(self, window: pd.DataFrame, trade_type: TradeType) -> float:
        return self.absolute_target_value
 