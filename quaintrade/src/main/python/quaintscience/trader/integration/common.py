from abc import ABC, abstractmethod
import datetime
import os
import copy
from threading import Lock
import http.server
import socketserver
from urllib.parse import urlparse, parse_qs

import talib
import pandas as pd
import redis

from ..core.logging import LoggerMixin
from ..core.ds import (Order,
                       Position,
                       TransactionType,
                       TradingProduct,
                       OrderType)
from ..core.util import today_timestamp, datestring_to_datetime


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
        super().__init__(*args, **kwargs)


    # Backtesting related

    def set_backtesting_time(self,
                             tim: datetime.datetime):
        self.backtesting_time = tim

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

    @abstractmethod
    def get_orders(self) -> list[Order]:
        pass

    @abstractmethod
    def place_order(self,
                    order: Order):
        pass

    def order_callback(self,
                       orders):
        raise NotImplementedError("Order Callback")

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
                            trigger_price: float = None):
        return Order(order_id=self.__new_id,
                     scrip_id=scrip,
                     exchange_id=exchange,
                     scrip=scrip,
                     exchange=exchange,
                     transaction_type=transaction_type,
                     timestamp=self.current_datetime(),
                     order_type = order_type,
                     product = product,
                     quantity = quantity,
                     trigger_price = trigger_price,
                     limit_price = limit_price)

    def place_express_order(self, 
                            scrip: str,
                            exchange: str,
                            quantity: int,
                            transaction_type: TransactionType = TransactionType.BUY,
                            product: TradingProduct = TradingProduct.MIS,
                            order_type: OrderType = OrderType.MARKET,
                            limit_price: float = None,
                            trigger_price: float = None):
        self.place_order(self.create_express_order(scrip=scrip,
                                                   exchange=exchange,
                                                   quantity=quantity,
                                                   transaction_type=transaction_type,
                                                   product=product,
                                                   order_type=order_type,
                                                   limit_price=limit_price,
                                                   trigger_price=trigger_price))

    @abstractmethod
    def place_another_order_on_entry(self, 
                                     entry_order: Order,
                                     other_order: Order):
        pass   

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
                    if ((fdate >= from_date and tdate <=to_date)
                        or (tdate >= from_date and tdate <= to_date)
                        or (fdate >= from_date and fdate <= to_date)):
                        this_fpath = os.path.join(dirname, f)
                        self.logger.info(f"Reading {this_fpath}")
                        data.append(self.read_data_file(this_fpath))
                data = pd.concat(data, axis=0) if len(data) > 0 else data[0]
                return self.postprocess_data(data, interval)
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
        data = data.resample(interval).apply({'open': 'first',
                                              'high': 'max',
                                              'low': 'min',
                                              'close': 'last'})
        data.dropna(inplace=True)
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
