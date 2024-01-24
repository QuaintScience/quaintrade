from abc import ABC, abstractmethod
from typing import Optional, Union
import copy
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
from .indicator import IndicatorPipeline

from .roles import Broker
from .ds import PositionType

class Strategy(ABC, LoggerMixin):

    NON_TRADING_FIRST_HOUR = [{"from": {"hour": 9,
                                        "minute": 0},
                               "to": {"hour": 9,
                                      "minute": 15}}]
    NON_TRADING_AFTERNOON = [{"from": {"hour": 15,
                                       "minute": 00},
                              "to": {"hour": 15,
                                     "minute": 59}}]

    def __init__(self,
                 indicator_pipeline: IndicatorPipeline,
                 *args,
                 default_interval: str = "3min",
                 non_trading_timeslots: list[dict[str, str]] = None,
                 intraday_squareoff: bool = True,
                 squareoff_hour: int = 15,
                 squareoff_minute: int = 00,
                 plottables: Optional[dict] = None,
                 default_tags: Optional[list] = None,
                 context_required: Optional[list[str]] = None,
                 max_budget: float = 10000,
                 min_quantity: int = 1,
                 long_position_tag: Optional[str] = None,
                 short_position_tag: Optional[str] = None,
                 trigger_price_cushion: float = 0.002,
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
        super().__init__(*args, **kwargs)

    @property
    def strategy_name(self):
        return f"{self.__class__.__name__}"

    def cancel_active_orders(self, broker: Broker,
                             scrip: Optional[str] = None,
                             exchange: Optional[str] = None,
                             product: Optional[TradingProduct] = None):
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
                broker.cancel_order(order, refresh_cache=True)
                broker.delete_gtt_orders_for(order)
                storage = broker.get_tradebook_storage()
                storage.store_order_execution(strategy=self.strategy_name,
                                              run_name=broker.run_name,
                                              run_id=broker.run_id,
                                              date=broker.current_datetime(),
                                              order=order,
                                              event="OrderCancelled")
                if order.parent_order_id is not None:
                    visited_parent_ids.add(order.parent_order_id)
        broker.cancel_invalid_child_orders()
        broker.cancel_invalid_group_orders()
        broker.get_orders(refresh_cache=True)
        return quantity

    def get_current_run(self, broker: Broker,
                        scrip: str, exchange: str):
        for order in broker.get_orders(refresh_cache=True):
            if (order.state == OrderState.PENDING
                and self.long_position_tag in order.tags
                and order.scrip == scrip
                and order.exchange == exchange):
                return TradeType.LONG
            elif (order.state == OrderState.PENDING
                and self.short_position_tag in order.tags
                and order.scrip == scrip
                and order.exchange == exchange):
                return TradeType.SHORT

    def perform_squareoff(self, broker: Broker,
                          scrip: Optional[str] = None,
                          exchange: Optional[str] = None,
                          product: Optional[TradingProduct] = None,
                          quantity: Optional[int] = None):

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
                                   window: pd.DataFrame):
        if (window.iloc[-1].name.hour >= self.squareoff_hour and
            window.iloc[-1].name.minute >= self.squareoff_minute
            and self.intraday_squareoff):
            self.cancel_active_orders(broker,
                                      product=TradingProduct.MIS)
            self.perform_squareoff(broker, product=TradingProduct.MIS)
            return True
        return False

    def can_trade(self, window: pd.DataFrame, context: dict[str, pd.DataFrame]):
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
                    return False
        return True

    def take_position(self,
                      scrip: str,
                      exchange: str,
                      broker: Broker,
                      position_type: PositionType,
                      trade_type: TradeType,
                      price: float,
                      quantity: int = 1,
                      product: TradingProduct = TradingProduct.MIS,
                      use_sl_market_order: bool = False,
                      tags: Optional[list] = None,
                      group_id: Optional[str] = None,
                      parent_order: Optional[Order] = None) -> Order:

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
                limit_price = price * (1 + self.trigger_price_cushion)
            else:
                transaction_type = TransactionType.SELL
                limit_price = price * (1 - self.trigger_price_cushion)

            limit_order_type = OrderType.SL_LIMIT if not use_sl_market_order else OrderType.SL_MARKET
            order = order_template(transaction_type=transaction_type,
                                   order_type=limit_order_type,
                                   trigger_price=price,
                                   limit_price=limit_price,
                                   tags=all_tags)
            order = broker.place_order(order)
        elif position_type == PositionType.STOPLOSS or position_type == PositionType.TARGET:

            if trade_type == TradeType.LONG:
                transaction_type = TransactionType.SELL
                limit_price = price * (1 - self.trigger_price_cushion)
            else:
                transaction_type = TransactionType.BUY
                limit_price = price * (1 + self.trigger_price_cushion)

            if position_type == PositionType.STOPLOSS:
                limit_order_type = OrderType.SL_LIMIT if not use_sl_market_order else OrderType.SL_MARKET
            else:
                limit_order_type = OrderType.LIMIT

            order = order_template(transaction_type=transaction_type,
                                   order_type=limit_order_type,
                                   trigger_price=price,
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

    def compute_indicators(self, df: pd.DataFrame):
        df, _, _ = self.indicator_pipeline.compute(df)
        return df

    def apply(self, broker: Broker,
              scrip: str,
              exchange: str,
              window: pd.DataFrame,
              context: dict[str, pd.DataFrame]) -> None:
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
