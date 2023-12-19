import datetime
from typing import Optional
from dataclasses import dataclass

import pandas as pd
from tabulate import tabulate

from ..core.ds import (Order,
                       Position,
                       OrderType,
                       TradingProduct,
                       OrderState,
                       TransactionType)
from .common import TradeManager
from ..core.util import default_dataclass_field


@dataclass(kw_only=True)
class PaperPosition(Position):
    quantity_and_price_history: list[(float, float)] = default_dataclass_field([])

    def __hash__(self):
        return hash(",".join([self.scrip, self.exchange, self.product.value]))

    def __eq__(self, other: object):
        if not isinstance(other, Position):
            return False
        if (self.scrip == other.scrip
            and self.exchange == other.exchange
            and self.product == other.product):
            return True
        return False


class PaperTradeManager(TradeManager):

    def __init__(self,
                 *args,
                 instruments: list = None,
                 load_from_files: bool = True,
                 load_from_redis: bool = False,
                 redis_load_frequency: float = 0.5,
                 historic_context_from: datetime.datetime = None,
                 historic_context_to: datetime.datetime = None,
                 interval: str = "10min",
                 refresh_orders_immediately_on_gtt_state_change: bool = False,
                 **kwargs):
        self.orders = []
        self.positions = {}
        self.gtt_orders = []
        if instruments is None:
            instruments = []
        self.instruments = instruments
        self.load_from_files = load_from_files
        self.load_from_redis = load_from_redis
        self.redis_load_frequency = redis_load_frequency
        if historic_context_from is None:
            historic_context_from = datetime.datetime.now() - datetime.timedelta(days=60)
        if historic_context_to is None:
            historic_context_to = datetime.datetime.now()
        self.historic_context_from = historic_context_from
        self.historic_context_to = historic_context_to
        self.interval = interval
        self.refresh_orders_immediately_on_gtt_state_change = refresh_orders_immediately_on_gtt_state_change
        kwargs["user_credentials"] = None
        self.data = {}
        self.idx = {}
        self.current_time = None
        self.events = []
        self.pnl_history = []
        super().__init__(*args, **kwargs)

    # Login related

    def start_login(self) -> str:
        self.auth_state = {"state": "Logged in"}
        return None

    def finish_login(self, *args, **kwargs) -> bool:
        pass

    # Initialization
    def init(self):
        if self.load_from_redis:
            ohlc_data = self.get_redis_tick_data_as_ohlc(refresh=True,
                                                         interval=self.interval)
            if isinstance(self.data, pd.Dataframe):
                self.data = pd.concat([self.data, ohlc_data],
                                      axis=0)
            else:
                self.data = ohlc_data
        if self.load_from_files:
            for instrument in self.instruments:
                data = self.get_historic_data(scrip=instrument["scrip"],
                                              exchange=instrument["exchange"],
                                              interval=self.interval,
                                              from_date=self.historic_context_from,
                                              to_date=self.historic_context_to,
                                              download=False)
                key = self.get_key_from_scrip(instrument["scrip"],
                                              instrument["exchange"])
                if key in self.data:
                    self.data[key] = pd.concat([self.data[key], data], axis=0).reset_index().drop_duplicates(subset='date', keep='first').set_index('date')
                else:
                    self.data[key] = data

    def current_datetime(self):
        return self.current_time

    def set_current_time(self, dt: datetime.datetime, traverse: bool = False,
                         instrument: Optional[dict] = None):
        for instrument in self.data.keys():
            to_idx = self.data[instrument].index.get_indexer([dt], method="nearest")[0]
            if self.data[instrument].iloc[to_idx].name < dt:
                to_idx += 1

            if to_idx >= len(self.data[instrument]):
                raise ValueError("Time exceeds last item in data for {instrument}")

            if not traverse:
                self.current_time = dt
                self.idx[instrument] = to_idx
            for idx in range(self.idx.get(instrument, 0) + 1, to_idx + 1):
                scrip, exchange = self.get_scrip_and_exchange_from_key(instrument)
                self.idx[instrument] = idx
                self.logger.debug(f"Incrementing time from {self.current_time} to {self.data[instrument].iloc[idx].name} for {instrument} idx={idx}")
                self.logger.debug(f"{self.current_time} OHLC "
                                  f" {self.data[instrument].iloc[idx]['open']}"
                                  f" {self.data[instrument].iloc[idx]['high']}"
                                  f" {self.data[instrument].iloc[idx]['low']}"
                                  f" {self.data[instrument].iloc[idx]['close']}")
                self.current_time = self.data[instrument].iloc[idx].name
                self.__process_orders(scrip=scrip,
                                      exchange=exchange)
                self.__update_positions()
                self.__process_orders(scrip=scrip,
                                      exchange=exchange)
                self.__update_positions()
                total_pnl = 0
                for key, value in self.positions.items():
                    total_pnl += value.pnl
                self.pnl_history.append([self.current_datetime(), total_pnl])

    def get_orders_as_table(self):
        status = [[self.current_time,
                       sum([1 for order in self.orders if order.state == OrderState.PENDING]),
                       sum([1 for order in self.orders if order.state == OrderState.COMPLETED]),
                       sum([1 for order in self.orders if order.state == OrderState.CANCELLED])]]
        print(tabulate(status, headers=["Time", "Pending", "Completed", "Cancelled"], tablefmt="double_outline"))
        
        printable_orders = []
        for order in self.orders:
            if order.state == OrderState.PENDING:
                printable_orders.append(["R",
                                         order.order_id[:4],
                                         order.parent_order_id[:4] if order.parent_order_id is not None else "",
                                         order.scrip,
                                         order.exchange,
                                         order.transaction_type,
                                         order.quantity,
                                         order.order_type,
                                         order.limit_price,
                                         ", ".join(order.tags)])
        for from_order, to_order in self.gtt_orders:
            printable_orders.append(["GTT",
                                     to_order.order_id[:4],
                                     from_order.order_id[:4],
                                     to_order.scrip,
                                     to_order.exchange,
                                     to_order.transaction_type,
                                     to_order.quantity,
                                     to_order.order_type,
                                     to_order.limit_price,
                                     ", ".join(to_order.tags)])
        headers = ["Typ", "id", "parent", "scrip", "exchange",
                   "buy/sell", "qty", "order_type", "limit_price", "reason"]
        print(tabulate(printable_orders, headers=headers, tablefmt="double_outline"))
        return printable_orders, headers

    def __update_positions(self):
        for _, position in self.positions.items():
            money_spent = sum(quantity * price
                              for price, quantity in position.quantity_and_price_history)
            cash_flow = sum(-quantity * price
                            for price, quantity in position.quantity_and_price_history)
            quantity_transacted = sum(abs(quantity)
                                      for _, quantity in position.quantity_and_price_history)
            net_quantity = sum(quantity
                               for _, quantity in position.quantity_and_price_history)
            position.average_price =  (abs(money_spent) / abs(net_quantity)) if abs(net_quantity) > 0 else 0
            key = self.get_key_from_scrip(position.scrip, position.exchange)
            #print(cash_flow, position.quantity_and_price_history)
            position.pnl = cash_flow + (net_quantity * self.data[key].iloc[self.idx[key]]["close"])

    def get_pnl_history_as_table(self):
        print(tabulate(self.pnl_history))
        return self.pnl_history

    def get_positions_as_table(self):
        # self.logger.debug(f"{self.current_time} entered __refresh_positions")
        self.__update_positions()
        printable_positions = []
        # print(self.positions)
        for _, position in self.positions.items():
            net_quantity = sum(quantity
                               for _, quantity in position.quantity_and_price_history)
            key = self.get_key_from_scrip(position.scrip, position.exchange)
            printable_positions.append([self.current_time,
                                        position.scrip,
                                        position.exchange,
                                        net_quantity,
                                        position.average_price,
                                        self.data[key].iloc[self.idx[key]]["close"],
                                        position.pnl])
            self.logger.info(f"{self.current_time} "
                             f"Position: {position.scrip}/{position.exchange} | {net_quantity} | {position.pnl:.2f}")
        headers = ["time", "scrip", "exchange", "qty", "avgP", "LTP", "PnL"]
        print(tabulate(printable_positions, headers=headers, tablefmt="double_outline"))
        return printable_positions, headers

    def __add_position(self,
                       order: Order,
                       last_price: float,
                       price: Optional[float] = None):
        order.state = OrderState.COMPLETED
        if price is None:
            price = order.limit_price
        order.price = price
        self.logger.info(f"Order {order.transaction_type.value} {order.order_id[:4]}/{order.scrip}/"
                         f"{order.exchange}/{order.order_type.value} [tags={order.tags}] @ {order.price} x {order.quantity} executed.")
        self.events.append([self.current_time,
                            {"scrip": order.scrip,
                             "exchange": order.exchange,
                             "transaction_type": order.transaction_type,
                             "quantity": order.quantity,
                             "price": price,
                             "event_type": ",".join(order.tags)}])
        # self.logger.info(self.events)
        position = PaperPosition(timestamp=self.current_time,
                                 scrip_id=order.scrip_id,
                                 scrip=order.scrip,
                                 exchange_id=order.exchange_id,
                                 exchange=order.exchange,
                                 product=order.product,
                                 average_price=price,
                                 last_price=last_price)

        # Position generates a hash if exchange, scrip, and product are the same
        # So updating position to all the latest values if it already exists..
        # print(self.positions)
        self.positions[position] = self.positions.get(position, position)
        position = self.positions[position]
        # print(position)
        if order.transaction_type == TransactionType.SELL:
            position.quantity -= order.quantity
            position.quantity_and_price_history.append((price, -order.quantity))
        else:
            position.quantity += order.quantity
            position.quantity_and_price_history.append((price, order.quantity))

    def __process_orders(self,
                         scrip=None,
                         exchange=None,
                         inside_a_recursion=False):
        # self.logger.debug(f"{self.current_time} entered __process_orders scrip={scrip} exchange={exchange}")
        for order in self.orders:
            if scrip is not None and exchange is not None:
                if order.scrip != scrip or order.exchange != exchange:
                    continue
            key = self.get_key_from_scrip(order.scrip, order.exchange)
            candle = self.data[key].iloc[self.idx[key]]
            if order.state == OrderState.PENDING:
                change = False
                if order.order_type in [OrderType.SL_LIMIT, OrderType.SL_MARKET]:
                    if order.transaction_type == TransactionType.BUY:
                        if candle["high"] >= order.trigger_price:
                            if order.order_type == OrderType.SL_LIMIT:
                                order.order_type = OrderType.LIMIT
                            else:
                                order.order_type = OrderType.MARKET
                    else:
                        if candle["low"] <= order.trigger_price:
                            if order.order_type == OrderType.SL_LIMIT:
                                self.logger.info(f"{order.order_id[:4]} Became LIMIT FROM SL_LIMIT")
                                change = True
                                order.order_type = OrderType.LIMIT
                            else:
                                order.order_type = OrderType.MARKET

                if order.order_type == OrderType.LIMIT:
                    if order.transaction_type == TransactionType.BUY:
                        if candle["low"] <= order.limit_price:
                            self.__add_position(order,
                                                last_price=candle["close"],
                                                price=min(order.limit_price,
                                                          candle["high"]))
                        self.cancel_invalid_child_orders()
                    elif order.transaction_type == TransactionType.SELL:
                        #if change:
                        #    print(order, candle)
                        if candle["high"] >= order.limit_price:
                             self.__add_position(order,
                                                 last_price=candle["close"],
                                                 price=max(order.limit_price,
                                                           candle["low"]))
                        self.cancel_invalid_child_orders()
                elif order.order_type == OrderType.MARKET:
                    self.__add_position(order, last_price=candle["close"], price=candle["close"])
                    self.cancel_invalid_child_orders()

                if order.state == OrderState.COMPLETED:
                    self.cancel_invalid_child_orders()
                    self.cancel_invalid_group_orders()
        new_gtt_orders = []
        gtt_state_changed = False
        for ii, (entry_order, other_order) in enumerate(self.gtt_orders):
            if scrip is not None and exchange is not None:
                if entry_order.scrip != scrip or entry_order.exchange != exchange:
                    continue
            if entry_order.state == OrderState.COMPLETED:
                self.orders.append(other_order)
                gtt_state_changed = True
                continue
            new_gtt_orders.append((entry_order, other_order))
        self.gtt_orders = new_gtt_orders

        if gtt_state_changed and self.refresh_orders_immediately_on_gtt_state_change:
            self.logger.info(f"{self.current_time} gtt_state_changed")
            self.__process_orders(scrip=scrip,
                                  exchange=exchange,
                                  inside_a_recursion=True)

        for position in self.positions:
            key = self.get_key_from_scrip(position.scrip, position.exchange)
            position.last_price = candle = self.data[key].iloc[self.idx[key]]["close"]
            position.pnl = (position.last_price - position.average_price) * position.quantity
        self.order_callback(self.orders)
        orders = []
        for order in self.orders:
            if order.state == OrderState.COMPLETED:
                continue
            orders.append(order)
        self.orders = orders

    def order_callback(self, orders):
        self.cancel_invalid_child_orders()
        self.cancel_invalid_group_orders()

    # Order streaming / management
    def get_orders(self) -> list[Order]:
        return self.orders

    def update_order(self, order: Order) -> Order:
        found = False
        for ii, other_order in enumerate(self.orders):
            if other_order.order_id == order.order_id:
                self.orders[ii] = order
                found = True
        if not found:
            raise KeyError(f"Order {order} not found.")

    def cancel_pending_orders(self):
        new_orders = []
        for order in self.orders:
            if order.state == OrderState.PENDING:
                continue
            new_orders.append(order)
        self.orders = new_orders

    def place_order(self,
                    order: Order) -> Order:
        self.orders.append(order)
        return order

    def cancel_order(self, order: Order) -> Order:
        for other_order in self.orders:
            if other_order.order_id == order.order_id:
                other_order.state = OrderState.CANCELLED

    def get_positions(self) -> list[Position]:
        return self.positions

    
    # Get Historical Data
    def download_historic_data(self,
                               scrip:str,
                               exchange: str,
                               interval: str,
                               from_date: datetime.datetime,
                               to_date: datetime.datetime) -> bool:
        raise NotImplementedError("paper trade manager cannot get any data")

    # Streaming data
    def start_realtime_ticks_impl(self, instruments: list, *args, **kwargs):
        raise NotImplementedError("paper trade manager cannot stream data")


    def on_connect_realtime_ticks(self, *args, **kwargs):
        raise NotImplementedError("paper trade manager cannot respond to streaming ticks")

    def on_close_realtime_ticks(self, *args, **kwargs):
        raise NotImplementedError("paper trade manager cannot respond to streaming ticks")
