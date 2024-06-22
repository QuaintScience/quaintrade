from abc import ABC, abstractmethod
from typing import Optional, Union
import copy
import datetime
from functools import partial
import os
import pickle
from functools import partial
import os
import pickle
from functools import partial

import pandas as pd

from .logging import LoggerMixin
from .ds import (Order,
                 TradeType,
                 OrderState,
                 OrderType,
                 TransactionType,
                 TradingProduct)
from .ds import (Order,
                 TradeType,
                 OrderState,
                 OrderType,
                 TransactionType,
                 TradingProduct)
from .indicator import IndicatorPipeline

from .roles import Broker
from .ds import PositionType

class Strategy(ABC, LoggerMixin):

    NON_TRADING_FIRST_HOUR = [{"from": {"hour": 9,
                                        "minute": 0},
                               "to": {"hour": 10,
                                      "minute": 15}}]
    NON_TRADING_AFTERNOON = [{"from": {"hour": 14,
                                       "minute": 00},
                              "to": {"hour": 15,
                                     "minute": 59}}]

    def __init__(self,
                 indicator_pipeline: IndicatorPipeline,
                 *args,
                 default_interval: str = "3min",
                 *args,
                 default_interval: str = "3min",
                 non_trading_timeslots: list[dict[str, str]] = None,
                 intraday_squareoff: bool = True,
                 squareoff_hour: int = 14,
                 squareoff_minute: int = 55,
                 plottables: Optional[dict] = None,
                 default_tags: Optional[list] = None,
                 context_required: Optional[list[str]] = None,
                 max_budget: float = 10000,
                 min_quantity: int = 1,
                 long_position_tag: Optional[str] = None,
                 short_position_tag: Optional[str] = None,
                 trigger_price_cushion: float = 0.002,
                 custom_plot_kwargs: Optional[dict] = None,
                 plot_context_candles: Optional[list[str]] = None,
                 **kwargs):
        
        self.indicator_pipeline = indicator_pipeline
        self.intraday_squareoff = intraday_squareoff
        self.squareoff_hour = squareoff_hour
        self.squareoff_minute = squareoff_minute
        if non_trading_timeslots is None:
            non_trading_timeslots = []
        self.non_trading_timeslots = non_trading_timeslots
        self.default_interval = default_interval
        if plottables is None:
            plottables = {"indicator_fields": []}
        if default_tags is None:
            default_tags = []
        if context_required is None:
            context_required = []
        self.plottables = plottables
        self.default_tags = default_tags
        self.context_required = context_required
        self.min_quantity = min_quantity
        self.max_budget = max_budget
        if long_position_tag is None:
            long_position_tag = f"{self.__class__.__name__}_long"
        if short_position_tag is None:
            short_position_tag = f"{self.__class__.__name__}_short"
        self.long_position_tag = long_position_tag
        self.short_position_tag = short_position_tag
        self.trigger_price_cushion = trigger_price_cushion
        self.custom_plot_kwargs = custom_plot_kwargs
        self.plot_context_candles = plot_context_candles
        super().__init__(*args, **kwargs)

    @property
    def strategy_name(self):
        return f"{self.__class__.__name__}"

    def cancel_active_orders(self, broker: Broker,
                             scrip: Optional[str] = None,
                             exchange: Optional[str] = None,
                             product: Optional[TradingProduct] = None,
                             delete_order_tags: Optional[list[str]] = None) -> int:

        quantity = 0
        visited_parent_ids = set()
        for order in broker.get_orders(refresh_cache=True):
            if (order.state == OrderState.PENDING
                and (self.long_position_tag in order.tags
                     or self.short_position_tag in order.tags)):
                if product is not None and order.product != product:
                    continue
                if scrip is not None and order.scrip != scrip:
                    continue
                if exchange is not None and order.exchange != exchange:
                    continue
                if (order.parent_order_id not in visited_parent_ids
                    and order.parent_order_id is not None
                    and ("target" in order.tags or "stoploss" in order.tags)):
                    quantity += order.quantity if order.transaction_type == TransactionType.SELL else -order.quantity
                    visited_parent_ids.add(order.parent_order_id)
                delete_order = False
                if delete_order_tags is not None:
                    for tag in delete_order_tags:
                        if tag in order.tags:
                            self.logger.debug(f"Deleting order {order.order_id} as it has tag {tag}")
                            delete_order = True
                            break
                else:
                    self.logger.debug(f"Deleting order {order.order_id} as no delete order tags are specified")
                    delete_order = True
                if delete_order:
                    broker.cancel_order(order, refresh_cache=True)
                    broker.delete_gtt_orders_for(order)
                    storage = broker.get_tradebook_storage()
                    storage.store_order_execution(strategy=self.strategy_name,
                                                run_name=broker.run_name,
                                                run_id=broker.run_id,
                                                date=broker.current_datetime(),
                                                order=order,
                                                event="OrderCancelled")

        broker.cancel_invalid_child_orders()
        broker.cancel_invalid_group_orders()
        broker.get_orders(refresh_cache=True)
        return quantity

    def is_new_context_bar(self,
                           candle: pd.Series,
                           context_candle: pd.Series,
                           context_name: str):

        td = datetime.timedelta(seconds=pd.Timedelta(context_name).total_seconds())
        return candle.name - context_candle.name == td

    def get_current_run(self, broker: Broker,
                        scrip: str,
                        exchange: str,
                        refresh_cache: bool = True) -> TradeType:
        current_target_order = self.get_current_position_order(broker,
                                                               scrip=scrip,
                                                               exchange=exchange,
                                                               product=self.product,
                                                               position_name="target",
                                                               refresh_order_cache=refresh_cache,
                                                               states=[OrderState.PENDING])
        current_stoploss_order = self.get_current_position_order(broker,
                                                                 scrip=scrip,
                                                                 exchange=exchange,
                                                                 product=self.product,
                                                                 position_name="target",
                                                                 refresh_order_cache=False,
                                                                 states=[OrderState.PENDING])
        if current_target_order is not None:
            if self.long_position_tag in current_target_order.tags:
                return TradeType.LONG
            elif self.short_position_tag in current_target_order.tags:
                return TradeType.SHORT
        if current_stoploss_order is not None:
            if self.long_position_tag in current_stoploss_order.tags:
                return TradeType.LONG
            elif self.short_position_tag in current_stoploss_order.tags:
                return TradeType.SHORT

        entry_order = self.get_current_position_order(broker,
                                                      scrip=scrip,
                                                      exchange=exchange,
                                                      product=self.product,
                                                      position_name="entry",
                                                      refresh_order_cache=False,
                                                      states=[OrderState.PENDING])
        if entry_order is not None:
            if self.long_position_tag in entry_order.tags:
                return TradeType.LONG
            elif self.short_position_tag in entry_order.tags:
                return TradeType.SHORT

    def get_current_position_order(self,
                                   broker: Broker,
                                   scrip: str,
                                   exchange: str,
                                   product: TradingProduct,
                                   position_name: str,
                                   refresh_order_cache: bool = True,
                                   states: list[OrderState] = None) -> Optional[Order]:
        if states is None:
            states = [OrderState.PENDING, OrderState.COMPLETED]
        latest_order = None
        for order in broker.get_orders(refresh_cache=refresh_order_cache):
            if (order.state in states
                and order.product == product
                and order.scrip == scrip
                and order.exchange == exchange
                and (self.long_position_tag in order.tags
                     or self.short_position_tag in order.tags)
                and position_name in order.tags):
                if latest_order is None or latest_order.timestamp < order.timestamp:
                    latest_order = order
        return latest_order

    def update_stoploss_order(self,
                              broker: Broker,
                              scrip: str,
                              exchange: str,
                              product: TradingProduct,
                              trigger_price: float,
                              refresh_order_cache: bool = True):
        
        entry_order = self.get_current_position_order(broker=broker,
                                                      scrip=scrip,
                                                      exchange=exchange,
                                                      product=product,
                                                      position="entry",
                                                      refresh_order_cache=False)
        stoploss_order = self.get_child_order(broker, entry_order, "stoploss")
        if stoploss_order is None:
            self.logger.warn("Could not find stoploss order for {entry_order}")
        stoploss_order.trigger_price = trigger_price
        if stoploss_order.transaction_type == TransactionType.SELL:
                stoploss_order.limit_price = trigger_price * (1 - self.trigger_price_cushion)
        else:
            stoploss_order.limit_price = trigger_price * (1 + self.trigger_price_cushion)
        broker.update_order(stoploss_order, refresh_cache=refresh_order_cache)
    
    def get_child_order(self,
                        broker: Broker,
                        entry_order: Order,
                        tag: str) -> Optional[Order]:
        for order in broker.get_orders(refresh_cache=False):
            if (order.parent_order_id is not None
                and order.parent_order_id == entry_order.order_id
                and (self.long_position_tag in order.tags
                     or self.short_position_tag in order.tags)
                and tag in order.tags):
                return order


    def perform_squareoff(self, broker: Broker,
                          scrip: Optional[str] = None,
                          exchange: Optional[str] = None,
                          product: Optional[TradingProduct] = None,
                          quantity: Optional[int] = None) -> None:

        positions = broker.get_positions(refresh_cache=True)
        for position in positions:
            if scrip is not None and exchange is not None:
                if position.scrip != scrip or position.exchange != exchange:
                    continue
            if product is not None and position.product != product:
                continue
            squareoff_transaction = None

            if quantity is None:
                if position.quantity > 0:
                    squareoff_transaction = TransactionType.SELL
                elif position.quantity < 0:
                    squareoff_transaction = TransactionType.BUY
            else:
                if quantity > 0:
                    if position.quantity < quantity and product in [TradingProduct.NRML, TradingProduct.CNC]:
                        self.logger.info(f"Did not square off {scrip} / {exchange} type {product}"
                                         f" as position qty {position.quantity} < sq qty {quantity}")
                        continue
                    squareoff_transaction = TransactionType.SELL
                elif quantity < 0:
                    squareoff_transaction = TransactionType.BUY

            squareoff_quantity = abs(position.quantity) if quantity is None else abs(quantity)

            if squareoff_transaction is not None:
                broker.place_express_order(scrip=position.scrip,
                                           exchange=position.exchange,
                                           quantity=squareoff_quantity,
                                           transaction_type=squareoff_transaction,
                                           order_type=OrderType.MARKET,
                                           tags=["squareoff_order"],
                                           strategy=self.strategy_name,
                                           run_name=broker.run_name,
                                           run_id=broker.run_id)
                self.logger.info(f"Squared off {position.quantity} in {position.scrip} with {squareoff_transaction}")

    def perform_intraday_squareoff(self,
                                   broker: Broker,
                                   window: pd.DataFrame) -> None:
        if (window.iloc[-1].name.hour >= self.squareoff_hour and
            window.iloc[-1].name.minute >= self.squareoff_minute
            and self.intraday_squareoff):
            self.cancel_active_orders(broker,
                                      product=TradingProduct.MIS)
            self.perform_squareoff(broker, product=TradingProduct.MIS)
            return True
        return False

    def can_trade(self, window: pd.DataFrame, context: dict[str, pd.DataFrame]) -> bool:
        return self.__can_trade_in_given_timeslot(window) and self.__can_trade_with_context(context)

    def __can_trade_with_context(self, context: dict[str, pd.DataFrame]):
        for key in self.context_required:
            if key not in context or len(context[key]) == 0:
                self.logger.info(f"Context {key} is empty. So not trading...")
                return False
        return True

    def __can_trade_in_given_timeslot(self, window: pd.DataFrame):
        row = window.iloc[-1]
        for non_trading_timeslot in self.non_trading_timeslots:
            if (row.name.hour > non_trading_timeslot["from"]["hour"]
                or (row.name.hour == non_trading_timeslot["from"]["hour"] and
                    row.name.minute >= non_trading_timeslot["from"]["minute"])):
                if (row.name.hour < non_trading_timeslot["to"]["hour"]
                    or (row.name.hour == non_trading_timeslot["to"]["hour"] and
                    row.name.minute <= non_trading_timeslot["to"]["minute"])):
            if (row.name.hour > non_trading_timeslot["from"]["hour"]
                or (row.name.hour == non_trading_timeslot["from"]["hour"] and
                    row.name.minute >= non_trading_timeslot["from"]["minute"])):
                if (row.name.hour < non_trading_timeslot["to"]["hour"]
                    or (row.name.hour == non_trading_timeslot["to"]["hour"] and
                    row.name.minute <= non_trading_timeslot["to"]["minute"])):
                    return False
        return True

    def take_position(self,
                      scrip: str,
                      exchange: str,
                      broker: Broker,
                      position_type: PositionType,
                      trade_type: TradeType,
                      limit_price: float,
                      trigger_price: Optional[float] = None,
                      quantity: int = 1,
                      product: TradingProduct = TradingProduct.MIS,
                      use_sl_market_order: bool = False,
                      entry_with_limit: bool = False,
                      tags: Optional[list] = None,
                      group_id: Optional[str] = None,
                      parent_order: Optional[Order] = None) -> Order:


        if trigger_price is None:
            trigger_price = limit_price
        if tags is None:
            tags = []

        all_tags = copy.deepcopy(self.default_tags)
        all_tags.extend(tags)
        all_tags = list(set(all_tags))
        all_tags.append(position_type.value)

        if trade_type == TradeType.LONG:
            all_tags.append(self.long_position_tag)
        elif trade_type == TradeType.SHORT:
            all_tags.append(self.short_position_tag)
            all_tags.append(self.long_position_tag)
        elif trade_type == TradeType.SHORT:
            all_tags.append(self.short_position_tag)
        else:
            raise ValueError(f"Don't know how to handle {trade_type}")
        order = None

        parent_id = None
        if parent_order is not None:
            parent_id = parent_order.order_id

        order_template = partial(broker.create_express_order,
                                 scrip=scrip,
                                 exchange=exchange,
                                 quantity=quantity,
                                 product=product,
                                 group_id=group_id,
                                 parent_order_id=parent_id)

        if position_type == PositionType.ENTRY:

            if trade_type == TradeType.LONG:
                transaction_type = TransactionType.BUY
            else:
                transaction_type = TransactionType.SELL

            limit_order_type = OrderType.SL_LIMIT if not use_sl_market_order else OrderType.SL_MARKET
            if entry_with_limit:
                limit_order_type = OrderType.LIMIT
            order = order_template(transaction_type=transaction_type,
                                   order_type=limit_order_type,
                                   trigger_price=trigger_price,
                                   limit_price=limit_price,
                                   tags=all_tags)
            order = broker.place_order(order)
        elif position_type == PositionType.STOPLOSS or position_type == PositionType.TARGET:

            if trade_type == TradeType.LONG:
                transaction_type = TransactionType.SELL
            else:
                transaction_type = TransactionType.BUY

            if position_type == PositionType.STOPLOSS:
                limit_order_type = OrderType.SL_LIMIT if not use_sl_market_order else OrderType.SL_MARKET
            else:
                limit_order_type = OrderType.LIMIT

            order = order_template(transaction_type=transaction_type,
                                   order_type=limit_order_type,
                                   trigger_price=trigger_price,
                                   limit_price=limit_price,
                                   tags=all_tags)
            order = broker.place_gtt_order(parent_order, order)
        else:
            raise ValueError(f"Cannot take position for {position_type}")
        self.logger.info(f"Strategy: Take Position: Order placed: {order}")
        return order


    def take_predefined_position(self,
                                 scrip: Union[str, dict[PositionType, str]],
                                 exchange: str,
                                 broker: Broker,
                                 trade_type: TradeType,
                                 entry_price: float,
                                 stoploss_price: float,
                                 target_price: float,
                                 quantity: int = 1,
                                 product: TradingProduct = TradingProduct.MIS,
                                 use_sl_market_order: bool = False,
                                 tags: Optional[list] = None,
                                 group_id: Optional[str] = None):
        
        take_position = partial(self.take_position,
                                scrip=scrip,
                                exchange=exchange,
                                broker=broker,
                                trade_type=trade_type,
                                quantity=quantity,
                                product=product,
                                use_sl_market_order=use_sl_market_order,
                                tags=tags,
                                group_id=group_id)

        entry_order = take_position(position_type=PositionType.ENTRY,
                                    price=entry_price)
        stoploss_order = take_position(position_type=PositionType.STOPLOSS,
                                       price=stoploss_price,
                                       parent_order=entry_order)
        target_order = take_position(position_type=PositionType.TARGET,
                                     price=target_price,
                                     parent_order=entry_order)
        return {"entry": entry_order, "stoploss": stoploss_order, "target": target_order}

    def apply(self, broker: Broker,
              scrip: str,
              exchange: str,
              window: pd.DataFrame,
              context: dict[str, pd.DataFrame]) -> None:

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
        for key, cdf in context.items():
            colvals = []
            for col in cdf.columns:
                if col not in ["open", "high", "low", "close"]:
                    colvals.append(f"{col}={cdf.iloc[-1][col]}")
            self.logger.info(f"CONTEXT {key} {self.__class__.__name__} [{cdf.iloc[-1].name}]:"
                            f" O={cdf.iloc[-1]['open']}"
                            f" H={cdf.iloc[-1]['high']}"
                            f" L={cdf.iloc[-1]['low']}"
                            f" C={cdf.iloc[-1]['close']}"
                            f" {' '.join(colvals)}")
        self.apply_impl(broker=broker,
                        scrip=scrip,
                        exchange=exchange,
                        window=window,
                        context=context)            
        self.perform_intraday_squareoff(broker=broker, window=window)

    @abstractmethod
    def apply_impl(self, broker: Broker,
                   scrip: str,
                   exchange: str,
                   window: pd.DataFrame,
                   context: dict[str, pd.DataFrame]) -> None:
        pass

    def apply_impl(self, broker: Broker,
                   scrip: str,
                   exchange: str,
                   window: pd.DataFrame,
                   context: dict[str, pd.DataFrame]) -> None:
        pass
