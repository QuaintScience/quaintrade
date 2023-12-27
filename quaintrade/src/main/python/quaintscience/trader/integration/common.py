from abc import ABC, abstractmethod
import datetime
import os
import copy
from threading import Lock
import http.server
import socketserver
from urllib.parse import urlparse, parse_qs
from typing import Optional

import pandas as pd
import redis

from ..core.logging import LoggerMixin
from ..core.ds import (Order,
                       Position,
                       TransactionType,
                       TradingProduct,
                       OrderType,
                       OrderState)
from ..core.util import today_timestamp, datestring_to_datetime, resample_candle_data


def CallbackHandleFactory(context):
    class CallbackHandler(http.server.BaseHTTPRequestHandler):

        def __init__(self, *args, **kwargs):
            self.context = context
            super().__init__(*args, **kwargs)

        def do_GET(self):
            self.send_response(200, "OK")
            self.end_headers()
            self.context["done"] = True
            self.context["query_params"] = parse_qs(urlparse(self.path).query)
    return CallbackHandler


class OAuthCallBackServer(socketserver.TCPServer):

    # Avoid "address already used" error when frequently restarting the script
    allow_reuse_address = True

    @staticmethod
    def get_oauth_callback_data(port):
        server_address = ('', port)
        context = {"done": False}
        HandlerClass = CallbackHandleFactory(context)
        with OAuthCallBackServer(server_address, HandlerClass) as httpd:
            while not context["done"]:
                httpd.handle_request()
        return context


class TradeManager(ABC, LoggerMixin):

    def __init__(self,
                 user_credentials: dict,
                 cache_path: str,
                 *args,
                 redis_server: str = None,
                 redis_port: int = 6379,
                 oauth_callback_port: int = 9595,
                 **kwargs):
        self.user_credentials = user_credentials
        self.cache_path = cache_path
        self.tick_data = {}
        self.kill_tick_thread = False
        self.redis_server = redis_server
        self.redis_port = redis_port
        self.tick_data_lock = Lock()
        self.oauth_callback_port = oauth_callback_port
        self.auth_state = {"state": "Not Logged In."}
        self.backtesting_time = datetime.datetime.now()
        if self.redis_server is not None:
            self.redis = redis.Redis(self.redis_server,
                                     self.redis_port)
        else:
            self.redis = None
        self.gtt_orders = []
        super().__init__(*args, **kwargs)

    # Utils

    def get_key_from_scrip(self,
                           scrip: str,
                           exchange: str):
        return f'{scrip.replace(":", " _")}:{exchange.replace(":", "_")}'

    def get_scrip_and_exchange_from_key(self, key: str):
        parts = key.split(":")
        return parts

    def current_datetime(self):
        return datetime.datetime.now()

    # Login related

    def listen_to_login_callback(self):
        return OAuthCallBackServer.get_oauth_callback_data(self.oauth_callback_port)

    @abstractmethod
    def start_login(self) -> str:
        pass

    @abstractmethod
    def finish_login(self,
                     *args,
                     **kwargs) -> bool:
        pass

    def get_state(self):
        return self.auth_state

    # Initialization

    @abstractmethod
    def init(self):
        pass

    # Order streaming / management

    def cancel_invalid_child_orders(self):
        for order in self.get_orders():
            if (order.parent_order_id is not None and
                order.state == OrderState.COMPLETED):
                for other_order in self.get_orders():
                    if (other_order.parent_order_id == order.parent_order_id and
                        other_order.order_id != order.order_id and
                        other_order.state == OrderState.PENDING):
                        self.logger.info(f"Cancelling order {order.order_id}/"
                                         f"{order.scrip}/{order.exchange}/"
                                         f"{order.transaction_type}/{order.order_type}"
                                         f"{','.join(order.tags)} due OCO (Sibling)")
                        self.cancel_order(other_order)
                        self.delete_gtt_orders_for(other_order)

    def cancel_invalid_group_orders(self):
        for order in self.get_orders():
            if (order.group_id is not None
                and order.state == OrderState.COMPLETED):
                for other_order in self.get_orders():
                    if (other_order.group_id == order.group_id
                        and other_order.order_id != order.order_id
                        and other_order.state == OrderState.PENDING
                        and "entry_order" in other_order.tags):
                        self.logger.info(f"Cancelling order {other_order.order_id[:4]}/"
                                         f"{other_order.scrip}/{other_order.exchange}/"
                                         f"{other_order.transaction_type}/{other_order.order_type}"
                                         f"{','.join(other_order.tags)} due OCO (Group)")
                        self.cancel_order(other_order)
                        self.delete_gtt_orders_for(other_order)

    @abstractmethod
    def get_orders_as_table(self) -> (list[list], list):
        pass

    @abstractmethod
    def get_positions_as_table(self) -> (list[list], list):
        pass

    @abstractmethod
    def get_orders(self) -> list[Order]:
        pass

    @abstractmethod
    def place_order(self,
                    order: Order) -> Order:
        pass

    def order_callback(self,
                       orders):
        raise NotImplementedError("Order Callback")

    @abstractmethod
    def update_order(self, order: Order) -> Order:
        pass

    @abstractmethod
    def cancel_pending_orders(self):
        pass

    @abstractmethod
    def cancel_order(self, order: Order) -> Order:
        pass

    @abstractmethod
    def get_positions(self) -> list[Position]:
        pass

    def create_express_order(self, 
                            scrip: str,
                            exchange: str,
                            quantity: int,
                            transaction_type: TransactionType = TransactionType.BUY,
                            product: TradingProduct = TradingProduct.MIS,
                            order_type: OrderType = OrderType.MARKET,
                            limit_price: float = None,
                            trigger_price: float = None,
                            tags: Optional[list] = None,
                            group_id: Optional[str] = None,
                            parent_order_id: Optional[str] = None) -> Order:
        if tags is None:
            tags = []
        return Order(scrip_id=scrip,
                     exchange_id=exchange,
                     scrip=scrip,
                     exchange=exchange,
                     transaction_type=transaction_type,
                     timestamp=self.current_datetime(),
                     order_type = order_type,
                     product = product,
                     quantity = quantity,
                     trigger_price = trigger_price,
                     limit_price = limit_price,
                     tags=tags,
                     group_id=group_id,
                     parent_order_id=parent_order_id)

    def place_express_order(self, 
                            scrip: str,
                            exchange: str,
                            quantity: int,
                            transaction_type: TransactionType = TransactionType.BUY,
                            product: TradingProduct = TradingProduct.MIS,
                            order_type: OrderType = OrderType.MARKET,
                            limit_price: float = None,
                            trigger_price: float = None,
                            group_id: Optional[str] = None,
                            parent_order_id: Optional[str] = None,
                            tags: Optional[list] = None) -> Order:
        order = self.create_express_order(scrip=scrip,
                                          exchange=exchange,
                                          quantity=quantity,
                                          transaction_type=transaction_type,
                                          product=product,
                                          order_type=order_type,
                                          limit_price=limit_price,
                                          trigger_price=trigger_price,
                                          group_id=group_id,
                                          parent_order_id=parent_order_id,
                                          tags=tags)
        order = self.place_order(order)
        return order

    def place_gtt_order(self,
                        entry_order: Order,
                        other_order: Order) -> (Order, Order):
        self.gtt_orders.append((entry_order, other_order))
        return entry_order, other_order

    def get_gtt_orders(self) -> list[(Order, Order)]:
        return self.gtt_orders

    def get_gtt_orders_for(self, order: Order) -> list[Order]:
        result = []
        for o1, o2 in self.get_gtt_orders():
            if o1.order_id == order.order_id:
                result.append(o2)
        return result

    def update_gtt_order(self,
                         entry_order: Order,
                         other_order: Order) -> (Order, Order):
        for ii, (o1, o2) in enumerate(self.gtt_orders):
            if (o1.order_id == entry_order.order_id
                and o2.order_id == other_order.order_id):
                self.gtt_orders[ii] == (entry_order, other_order)
        return entry_order, other_order

    def delete_gtt_orders_for(self, order: Order):
        new_gtt_orders = []
        for o1, o2 in self.get_gtt_orders():
            if o1.order_id == order.order_id:
                continue
            new_gtt_orders.append((o1, o2))
        self.gtt_orders = new_gtt_orders


    def clear_gtt_orders(self):
        self.gtt_orders =[]

    # Get Historical Data


    @abstractmethod
    def download_historic_data(self,
                               scrip:str,
                               exchange: str,
                               interval: str,
                               from_date: datetime.datetime,
                               to_date: datetime.datetime) -> bool:
        pass

    def get_historic_data(self,
                          scrip:str,
                          exchange: str,
                          interval: str,
                          from_date: datetime.datetime,
                          to_date: datetime.datetime,
                          download: bool = False) -> pd.DataFrame:
        filepath = self.get_filepath_for_data(scrip, exchange, from_date, to_date)
        if os.path.exists(filepath):
            return self.postprocess_data(self.read_data_file(filepath), interval)
        else:
            if not download:
                dirname = os.path.dirname(filepath)
                files = os.listdir(dirname)
                data = []
                for f in files: 
                    f_base = ".".join(f.split(".")[:-1])
                    if len(f_base.split("-")) == 5:
                        _, scrip, exchange, fdate, tdate = f_base.split("-")
                    else:
                        scrip, exchange, fdate, tdate = f_base.split("-")
                    fdate = datestring_to_datetime(fdate)
                    tdate = datestring_to_datetime(tdate)
                    # print(fdate, tdate, from_date, to_date, (fdate >= from_date and tdate <=to_date), (tdate >= from_date and tdate <= to_date), (fdate >= from_date and fdate <= to_date))
                    if ((fdate <= from_date and tdate >= to_date)
                        or (tdate >= from_date and tdate <= to_date)
                        or (fdate >= from_date and fdate <= to_date)):
                        this_fpath = os.path.join(dirname, f)
                        self.logger.info(f"Reading {this_fpath}")
                        data.append(self.read_data_file(this_fpath))
                data = pd.concat(data, axis=0) if len(data) > 1 else data[0]
                data = self.postprocess_data(data, interval)
                self.logger.info(f"Read {len(data)} rows.")
                return data.loc[(data.index >= from_date) & (data.index <= to_date)]
            else:
                self.download_historic_data(scrip,
                                            exchange,
                                            interval,
                                            from_date,
                                            to_date)
                return self.get_historic_data(scrip,
                                              exchange,
                                              interval,
                                              from_date,
                                              to_date,
                                              download=False)

    def read_data_file(self, filepath, **kwargs):
        return pd.read_csv(filepath, index_col="date", **kwargs)

    def get_filepath_for_data(self, scrip, exchange, from_date, to_date):
        sanitized_scrip = scrip.replace("-",
                                        "_").replace(" ",
                                                     "_")
        sanitized_exchange = exchange.replace("-",
                                              "_").replace(" ",
                                                           "_")
        filepath = os.path.join(self.cache_path,
                                "historical_data",
                                exchange,
                                sanitized_scrip,
                                f"data-{sanitized_scrip}-{sanitized_exchange}-"
                                f"{from_date.year:04d}{from_date.month:02d}{from_date.day:02d}"
                                f"-{to_date.year:04d}{to_date.month:02d}{to_date.day:02d}.csv")
        return filepath

    def postprocess_data(self,
                         data,
                         interval):
        data.fillna(0., inplace=True)
        if "time" in data.columns and "date" in data.columns:
            data["timestamp"] = pd.to_datetime(data["date"] + ", " + data["time"])
            data.drop(["date", "time"], inplace=True, axis=1)
            data.index = data["timestamp"]
        data.dropna(inplace=True)
        data.index = pd.to_datetime(data.index)
        data = resample_candle_data(data, interval)
        return data

    # Streaming data

    def start_realtime_ticks(self, instruments: list, *args, **kwargs):
        self.load_tick_data_from_redis()
        self.start_realtime_ticks_impl(instruments, *args, **kwargs)

    @abstractmethod
    def start_realtime_ticks_impl(self, instruments: list, *args, **kwargs):
        pass

    def on_connect_realtime_ticks(self, *args, **kwargs):
        raise NotImplementedError("On Connect Realtime Ticks")

    def on_close_realtime_ticks(self, *args, **kwargs):
        raise NotImplementedError("On Close Realtime Ticks")

    def stop_realtime_ticks(self):
        self.kill_tick_thread = True

    def on_tick(self,
                token,
                ltp,
                ltq,
                ltt,
                key,
                *args,
                **kwargs):
        token = str(token)
        key = str(key)
        with self.tick_data_lock:
            if not token in self.tick_data:
                try:
                    self.tick_data[token] = {}
                except:
                    self.logger.error(f"Error creating data from {self.tick_data}")
            if key not in self.tick_data[token]:
                self.tick_data[token][key] = {"date": key,
                                              "open": ltp,
                                              "high": ltp,
                                              "low": ltp,
                                              "close": ltp,
                                              "volume": ltq,
                                              "oi": 0.}
            if ltp > self.tick_data[token][key]["high"]:
                self.tick_data[token][key]["high"] = ltp
            if ltp < self.tick_data[token][key]["low"]:
                self.tick_data[token][key]["low"] = ltp
            self.tick_data[token][key]["close"] = ltp
            self.tick_data[token][key]["volume"] += ltq
            self.store_tick_data_to_redis()

    def load_tick_data_from_redis(self):
        self.tick_data = self.redis.json().get(f'kite-ticks-{today_timestamp()}', '$')
        if self.tick_data is None:
            self.logger.info("Tick data is empty")
            self.tick_data = {}
        if isinstance(self.tick_data, list):
            self.tick_data = self.tick_data[0]

    def get_redis_tick_data_as_ohlc(self, refresh: bool = True, interval: str = "1m") -> list[pd.DataFrame]:
        if refresh:
            self.load_tick_data_from_redis()
        dfs = {}
        for instrument, data in self.tick_data.items():
            df = pd.DataFrame.from_dict(data, orient='index')
            df.index = pd.to_datetime(df.index)
            dfs[instrument] = self.postprocess_data(df, interval)
        return dfs

    def store_tick_data_to_redis(self):
        self.redis.json().set(f'kite-ticks-{today_timestamp()}', '$', self.tick_data)

    def get_tick_data(self, instruments=None):
        with self.tick_data_lock:
            if instruments is None:
                return copy.deepcopy(self.tick_data)
            result = {}
            for instrument in instruments:
                if instrument["exchange"] not in result:
                    result[instrument["exchange"]] = {}
                this_tick_data = self.tick_data[self.get_key_from_scrip(instrument["scrip"],
                                                                        instrument["exchange"])]
                result[instrument["exchange"]][instrument["scrip"]] = copy.deepcopy(this_tick_data)
            return result

    # Backtesting specific methods
