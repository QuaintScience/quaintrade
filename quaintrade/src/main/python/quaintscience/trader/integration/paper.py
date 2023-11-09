import uuid
import datetime
from typing import Optional
from dataclasses import dataclass

import pandas as pd

from ..core.ds import Order, Position, OrderType, TradingProduct, OrderState, TransactionType
from .common import TradeManager



@dataclass
class PaperPosition(Position):
    quantity_and_price_history: list[(float, float)]

class PaperTradeManager(TradeManager):

    def __init__(self,
                 instruments: list,
                 *args,
                 load_from_files: bool = True,
                 load_from_redis: bool = False,
                 redis_load_frequency: float = 0.5,
                 historic_context_from: datetime.datetime = None,
                 historic_context_to: datetime.datetime = None,
                 min_interval: str = "1min",
                 **kwargs):
        self.orders = []
        self.positions = {}
        self.gtt_orders = []
        self.instruments = instruments
        self.load_from_files = load_from_files
        self.load_from_redis = load_from_redis
        self.redis_load_frequency = redis_load_frequency
        self.historic_context_from = historic_context_from
        self.historic_context_to = historic_context_to
        self.min_interval = min_interval
        super().__init__(*args, **kwargs)

    # Util

    @property
    def __new_id(self):
        return str(uuid.uuid4().replace("-",""))

    # Login related

    def start_login(self) -> str:
        self.auth_state = {"state": "Logged in"}
        return None

    def finish_login(self, *args, **kwargs) -> bool:
        pass

    # Initialization
    def init(self):
        if self.load_from_redis:
            ohlc_data = self.get_redis_tick_data_as_ohlc(refresh=True, interval='1min')
            if isinstance(self.data, pd.Dataframe):
                self.data = pd.concat([self.data, ohlc_data],
                                      axis=0)
            else:
                self.data = ohlc_data
        if self.load_from_files:
            for instrument in self.instruments:
                data = self.get_historic_data(scrip=instrument["scrip"],
                                              exchange=instrument["exchange"],
                                              interval=self.min_interval,
                                              from_date=self.historic_context_from,
                                              to_date=self.historic_context_to,
                                              download=False)
                key = self.get_key_from_scrip(instrument["scrip"],
                                              instrument["exchange"])
                if key in self.data:
                    self.data[key] = pd.concat([self.data[key], data], axis=0)
                else:
                    self.data[key] = data

    def set_current_time(self, scrip, exchange, dt: datetime.datatime, traverse: bool = False):
        for instrument in self.data.keys():
            to_idx = self.data[instrument].index.get_loc(dt, method="nearest")
            if self.data[instrument].iloc[to_idx].index < dt:
                to_idx += 1
            if to_idx >= len(self.data):
                raise ValueError("Time exceeds last item in data for {instrument}")

            if not traverse:
                self.current_time = dt
                self.idx[instrument] = to_idx
                return
            scrip, exchange = self.get_scrip_and_exchange_from_key(instrument)
            for idx in range(self.idx[instrument], to_idx):
                self.idx[instrument] = idx
                self.__process_orders(scrip=scrip,
                                      exchange=exchange)

    def __refresh_positions(self):
        for _, value in self.positions.items():
            money_spent = sum(abs(quantity) * price
                              for price, quantity in value.quantity_and_price_history)
            cash_flow = sum(quantity * price
                            for price, quantity in value.quantity_and_price_history)
            quantity_transacted = sum(abs(quantity)
                                      for _, quantity in value.quantity_and_price_history)
            value.average_price =  money_spent / quantity_transacted
            value.pnl = cash_flow

    def __add_position(self,
                       order: Order,
                       last_price: float,
                       price: Optional[float] = None):
        order.state = OrderState.COMPLETED
        if price is None:
            price = order.limit_price
        
        position = PaperPosition(position_id=self.__new_id,
                                 timestamp=self.current_time,
                                 scrip_id=order.scrip_id,
                                 scrip=order.scrip,
                                 exchange_id=order.exchange_id,
                                 exchange=order.exchange,
                                 product=order.product,
                                 quantity=0,
                                 average_price=price,
                                 last_price=last_price,
                                 pnl=0,
                                 day_change=0,
                                 raw_dict={})

        # Position generates a hash if exchange, scrip, and product are the same
        # So updating position to all the latest values if it already exists..
        self.positions[position] = self.positions.get(position, position)
        position = self.positions[position]
        if order.transaction_type == TransactionType.SELL:
            position.quantity -= order.quantity
            position.quantity_and_price_history.append((-order.quantity, order.price))
        else:
            position.quantity += order.quantity
            position.quantity_and_price_history.append((order.quantity, order.price))

    def __process_orders(self, scrip=None, exchange=None):

        for order in self.orders:
            if scrip is not None and exchange is not None:
                if order.scrip != scrip or order.exchange != exchange:
                    continue
            key = self.get_key_from_scrip(order.scrip, order.exchange)
            candle = self.data[key].iloc[self.idx[key]]
            if order.state == OrderState.PENDING:
                if order.order_type in [OrderType.SL_LIMIT, OrderType.SL_MARKET]:
                    if order.transaction_type == TransactionType.BUY:
                        if candle["low"] > order.trigger_price:
                            if order.order_type == OrderType.SL_LIMIT:
                                order.order_type = OrderType.LIMIT
                            else:
                                order.order_type = OrderType.MARKET
                    else:
                        if candle["high"] < order.trigger_price:
                            if order.order_type == OrderType.SL_LIMIT:
                                order.order_type = OrderType.LIMIT
                            else:
                                order.order_type = OrderType.MARKET

                if order.order_type == OrderType.LIMIT:
                    if candle["high"] <= order.limit_price and candle["low"] <= order.limit_price:
                        self.__add_position(order)
                elif order.order_type == OrderType.MARKET:
                    self.__add_position(order, price=candle["close"])

        new_gtt_orders = []
        gtt_executed = False
        for ii, (entry_order, other_order) in enumerate(self.gtt_orders):
            if scrip is not None and exchange is not None:
                if entry_order.scrip != scrip or entry_order.exchange != exchange:
                    continue
            if entry_order.state == OrderState.COMPLETED:
                self.orders.append(other_order)
                gtt_executed = True
                continue
            new_gtt_orders.append((entry_order, other_order))
        self.gtt_orders = new_gtt_orders
        if gtt_executed:
            self.__process_orders(scrip=scrip,
                                  exchange=exchange)

        for position in self.positions:
            key = self.get_key_from_scrip(position.scrip, position.exchange)
            position.last_price = candle = self.data[key].iloc[self.idx[key]]["close"]
            position.pnl = (position.last_price - position.average_price) * position.quantity
        self.__refresh_positions()

    # Order streaming / management
    def get_orders(self) -> list[Order]:
        return self.orders

    def place_order(self,
                    order: Order):
        self.orders.append(order)

    def order_callback(self, orders):
        raise NotImplementedError("Order Callback not supported in paper trader")

    def get_positions(self) -> list[Position]:
        return self.positions

    def place_another_order_on_entry(self,
                                     entry_order: Order,
                                     other_order: Order):
        self.gtt_orders.append((entry_order, other_order))

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
