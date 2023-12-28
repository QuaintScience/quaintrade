from abc import ABC, abstractmethod
from typing import Optional, Union
import copy
from functools import partial
from collections import defaultdict

import pandas as pd

from .logging import LoggerMixin
from .ds import (Order,
                 TradeType,
                 OrderType,
                 TransactionType,
                 ExecutionType,
                 TradingProduct)
from .graphing import backtesting_results_plot
from .indicator import IndicatorPipeline
from .util import resample_candle_data
from ..integration.common import TradeManager
from ..integration.paper import PaperTradeManager


class StrategyExecutor(ABC, LoggerMixin):

    NON_TRADING_FIRST_HOUR = [{"from": {"hour": 9,
                                        "minute": 0},
                               "to": {"hour": 9,
                                      "minute": 29}}]
    NON_TRADING_AFTERNOON = [{"from": {"hour": 15,
                                       "minute": 00},
                              "to": {"hour": 15,
                                     "minute": 59}}]

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
                 squareoff_minute: int = 00,
                 execution_type: ExecutionType = ExecutionType.BACKTESTING,
                 moving_window_size: int = 2,
                 trade_manager: TradeManager = None,
                 plot_results: bool = True,
                 indicator_fields: Optional[list] = None,
                 use_sl_market: bool = False,
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
        self.plot_results = plot_results
        if indicator_fields is None:
            indicator_fields = []
        self.indicator_fields = indicator_fields
        self.order_journal = []
        self.use_sl_market = use_sl_market
        super().__init__(*args, **kwargs)

    def perform_squareoff(self):

        self.trade_manager.cancel_pending_orders()
        positions = self.trade_manager.get_positions()
        #print(">>>> Positions")
        #print(positions)
        for position in positions:
            if position.quantity > 0:
                self.trade_manager.place_express_order(scrip=position.scrip,
                                                        exchange=position.exchange,
                                                        quantity=position.quantity,
                                                        transaction_type=TransactionType.SELL,
                                                        order_type=OrderType.MARKET,
                                                        tags=["squareoff_order"])
                self.logger.info(f"Squared off {position.quantity} in {position.scrip} with SELL")
            elif position.quantity < 0:
                self.trade_manager.place_express_order(scrip=position.scrip,
                                                        exchange=position.exchange,
                                                        quantity=-position.quantity,
                                                        transaction_type=TransactionType.BUY,
                                                        order_type=OrderType.MARKET,
                                                        tags=["squareoff_order"])
                self.logger.info(f"Squared off {position.quantity} in {position.scrip} with BUY")
        self.trade_manager.clear_gtt_orders()

    def perform_intraday_squareoff(self, window: pd.DataFrame):
        if (window.iloc[-1].name.hour >= self.squareoff_hour and
            window.iloc[-1].name.minute >= self.squareoff_minute
            and self.intraday_squareoff):
            self.perform_squareoff()
            return True
        return False

    def can_trade(self, window: pd.DataFrame):
        row = window.iloc[-1]
        for non_trading_timeslot in self.non_trading_timeslots:
            if (row.name.hour > non_trading_timeslot["from"]["hour"]
                or (row.name.hour == non_trading_timeslot["from"]["hour"] and
                    row.name.minute >= non_trading_timeslot["from"]["minute"])):
                if (row.name.hour < non_trading_timeslot["to"]["hour"]
                    or (row.name.hour == non_trading_timeslot["to"]["hour"] and
                    row.name.minute <= non_trading_timeslot["to"]["minute"])):
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

    def take_position(self, window: pd.DataFrame,
                      trade_type: TradeType,
                      entry_price_func: Optional[callable] = None,
                      entry_trigger_price_func: Optional[callable] = None,
                      sl_price_func: Optional[callable] = None,
                      sl_trigger_price_func: Optional[callable] = None,
                      target_price_func: Optional[callable] = None,
                      tags: Optional[list] = None,
                      group_id: Optional[str] = None):
        if entry_price_func is None:
            entry_price_func = self.get_entry
        if entry_trigger_price_func is None:
            entry_trigger_price_func = entry_price_func
        if sl_trigger_price_func is None:
            sl_trigger_price_func = sl_price_func
        if tags is None:
            tags = []

        entry_tags = copy.deepcopy(tags)
        entry_tags.append("entry_order")

        sl_tags = copy.deepcopy(tags)
        sl_tags.append("sl_order")

        target_tags = copy.deepcopy(tags)
        target_tags.append("target_order")

        scrip = None
        if trade_type == TradeType.LONG:
            scrip = self.long_scrip
            transaction_type = TransactionType.BUY
        else:
            scrip = self.short_scrip
            transaction_type = TransactionType.SELL
        order_template = partial(self.trade_manager.create_express_order,
                                 scrip=scrip,
                                 exchange=self.exchange,
                                 quantity=self.get_quantity(window, trade_type),
                                 product=self.product,
                                 group_id=group_id)
        limit_order_type = OrderType.SL_LIMIT if not self.use_sl_market else OrderType.SL_MARKET
        entry_order = order_template(transaction_type=transaction_type,
                                     order_type=limit_order_type,
                                     trigger_price=entry_trigger_price_func(window=window,
                                                                            trade_type=trade_type),
                                     limit_price=entry_price_func(window=window,
                                                                  trade_type=trade_type),
                                     tags=entry_tags)
        entry_order = self.trade_manager.place_order(entry_order)

        stoploss_order = None
        if sl_price_func is not None:
            if trade_type == TradeType.LONG:
                transaction_type = TransactionType.SELL
            else:
                transaction_type = TransactionType.BUY
            sl_price = sl_price_func(window=window, trade_type=trade_type)
            sl_trigger_price = sl_trigger_price_func(window=window, trade_type=trade_type)
            stoploss_order = order_template(transaction_type=transaction_type,
                                            order_type=limit_order_type,
                                            limit_price=sl_price,
                                            trigger_price=sl_trigger_price,
                                            tags=sl_tags)
            stoploss_order = self.trade_manager.place_gtt_order(entry_order, stoploss_order)
            stoploss_order[1].parent_order_id = entry_order.order_id
        target_order = None
        if target_price_func is not None:
            target_order = order_template(transaction_type=transaction_type,
                                        order_type=OrderType.LIMIT,
                                        limit_price=target_price_func(window=window,
                                                                        trade_type=trade_type),
                                        tags=target_tags)
            target_order = self.trade_manager.place_gtt_order(entry_order, target_order)
            target_order[1].parent_order_id = entry_order.order_id
        self.order_journal.append({"entry": entry_order,
                                   "sl": stoploss_order,
                                   "target": target_order,
                                   "timestamp": window.iloc[-1].index})
        # print(self.order_journal[-1])

    @abstractmethod
    def strategy(self, window: pd.DataFrame,
                 context: dict[str, pd.DataFrame]) -> Optional[TradeType]:
        pass

    def trade(self,
              df: pd.DataFrame,
              context_df: Optional[pd.DataFrame] = None,
              context_sampling_interval: Optional[str] = None,
              stream: bool = False) -> list[Order]:
        did_squareoff = False

        df, _, _ = self.indicator_pipeline.compute(df)
        pnls = defaultdict(list)

        daily_context = resample_candle_data(context_df, "1d")
        daily_context = self.indicator_pipeline.compute(daily_context)[0]
        
        weekly_context = resample_candle_data(context_df, "1w")
        weekly_context = self.indicator_pipeline.compute(weekly_context)[0]
        
        hourly_context = resample_candle_data(context_df, "1h")
        hourly_context = self.indicator_pipeline.compute(hourly_context)[0]

        bihourly_context = resample_candle_data(context_df, "2h")
        bihourly_context = self.indicator_pipeline.compute(bihourly_context)[0]

        trihourly_context = resample_candle_data(context_df, "3h")
        trihourly_context = self.indicator_pipeline.compute(trihourly_context)[0]

        if not stream:
            ts = None
            for ii in range(1, len(df) - self.moving_window_size + 1, 1):
                window = df.iloc[ii: ii + self.moving_window_size]
                if ts is None or ts.day != window.iloc[-1].name.day:
                    self.logger.debug(f"Trading at {ts}")

                now_tick = window.iloc[-1].name.to_pydatetime()
                
                if self.execution_type == ExecutionType.BACKTESTING:
                    self.trade_manager.set_current_time(now_tick,
                                                        traverse=True)
                if did_squareoff:
                    if self.execution_type == ExecutionType.BACKTESTING:        
                        for k, v in self.trade_manager.positions.items():
                            pnls[f"{v.scrip}:{v.exchange}"].append(v.pnl)
                did_squareoff = self.perform_intraday_squareoff(window)

                ts = window.iloc[-1].name
                context = {"1d": daily_context[daily_context.index < now_tick],
                           "1w": weekly_context[weekly_context.index < now_tick],
                           "1h": hourly_context[hourly_context.index < now_tick],
                           "2h": bihourly_context[bihourly_context.index < now_tick],
                           "3h": trihourly_context[trihourly_context.index < now_tick]}
                trade_type = self.strategy(window, context=context)
                if trade_type is not None:
                    self.logger.info(f"Found trade {trade_type} at {ts} {window.iloc[-1]['close']}")
                    if not self.can_trade(window):
                        self.logger.info(f"Cannot trade at {ts}")
                    self.take_position(window, trade_type)

                self.logger.info("--------------Tables After Strategy Computation Start-------------")
                self.trade_manager.get_orders_as_table()
                self.trade_manager.get_positions_as_table()
                self.logger.info("--------------Tables After Strategy Computation End-------------")
                
                #self.trade_manager.positions = {}
            if self.execution_type == ExecutionType.BACKTESTING:
                # Plot candlestick + strategy markers + PnL
                if self.plot_results:
                    pnl_history = self.trade_manager.get_pnl_history_as_table()
                    pnl_history = pd.DataFrame(pnl_history)
                    pnl_history = pnl_history.set_index(pnl_history.columns[0])
                    df["pnl_history"] = pnl_history[pnl_history.columns[0]]
                    print(df["pnl_history"])
                    self.indicator_fields.append({"field": "pnl_history", "panel": 1})
                    backtesting_results_plot(df, events=self.trade_manager.events,
                                            indicator_fields=self.indicator_fields)
            print(pnls)

        else:
            if self.can_trade(df):
                context = {"1d": daily_context,
                           "1w": weekly_context,
                           "1h": hourly_context,
                           "2h": bihourly_context,
                           "3h": trihourly_context}
                trade_type = self.strategy(df, context)
                if trade_type is not None:
                    self.logger.info(f"Found trade {trade_type} at {ts}")
                    self.take_position(df, trade_type)
            did_squareoff = self.perform_squareoff(df)


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
                 long_entry_price_column: str,
                 short_entry_price_column: str,
                 *args,
                 idx: int = -1,
                 extra: float = 1,
                 **kwargs):
        self.long_entry_price_column = long_entry_price_column
        self.short_entry_price_column = short_entry_price_column
        self.extra = extra
        self.idx = idx
        super().__init__(*args, **kwargs)

    def get_entry(self, window: pd.DataFrame, trade_type: TradeType):
        x = None
        if trade_type == TradeType.LONG:
            x = window.iloc[self.idx][self.long_entry_price_column]
        else:
            x = window.iloc[self.idx][self.short_entry_price_column]
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
                 sl_price_column_long: str,
                 sl_price_column_short: str,
                 target_price_column_long: str,
                 target_price_column_short: str,
                 *args,
                 **kwargs):
        self.relative_stoploss_value = relative_stoploss_value
        self.relative_target_value = relative_target_value
        self.sl_price_column_long = sl_price_column_long
        self.sl_price_column_short = sl_price_column_short
        self.target_price_column_long = target_price_column_long
        self.target_price_column_short = target_price_column_short
        super().__init__(*args, **kwargs)

    def get_stoploss(self, window: pd.DataFrame, trade_type: TradeType) -> float:
        if trade_type == TradeType.LONG:
            return window.iloc[-1][self.sl_price_column_long] - self.relative_stoploss_value
        else:
            return window.iloc[-1][self.sl_price_column_short] + self.relative_stoploss_value
    
    def get_target(self, window: pd.DataFrame, trade_type: TradeType) -> float:
        if trade_type == TradeType.LONG:
            return window.iloc[-1][self.target_price_column_long] + self.relative_target_value
        else:
            return window.iloc[-1][self.target_price_column_short] - self.relative_target_value


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
 