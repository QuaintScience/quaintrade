from abc import ABC, abstractmethod
from typing import Union, Type
import datetime
import os
import copy
from threading import Lock
from collections import defaultdict
import http.server
import socketserver
from urllib.parse import urlparse, parse_qs
from typing import Optional

import pandas as pd
import redis

from .logging import LoggerMixin
from .ds import (Order,
                       Position,
                       TransactionType,
                       TradingProduct,
                       OrderType,
                       OrderState,
                       OHLCStorageType)
from .util import resample_candle_data, get_scrip_and_exchange_from_key
from .persistence import SqliteOHLCStorage, OHLCStorage


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


class AuthenticatorMixin():
    

    def __init__(self,
                 auth_credentials: dict,
                 auth_cache_filepath: str,
                 *args,
                 reset_auth_cache: bool = False,
                 oauth_callback_port: int = 9595,
                 **kwargs):
        self.auth_credentials = auth_credentials
        self.oauth_callback_port = oauth_callback_port
        self.auth_cache_filepath = auth_cache_filepath
        self.reset_auth_cache = reset_auth_cache
        self.auth_state = {"state": "Not Logged In."}


    def listen_to_login_callback(self):
        return OAuthCallBackServer.get_oauth_callback_data(self.oauth_callback_port)

    @abstractmethod
    def login(self) -> str:
        pass


class TradingServiceProvider(ABC, LoggerMixin):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @abstractmethod
    def init(self):
        pass

    def current_datetime(self):
        return datetime.datetime.now()


class DataProvider(TradingServiceProvider):

    ProviderName = "abstract"

    def __init__(self,
                 data_path: str,
                 *args,
                 StorageClass: Union[str, Type[OHLCStorage]] = SqliteOHLCStorage,
                 **kwargs):
        self.data_path = data_path
        self.StorageClass = StorageClass
        super().__init__(*args, **kwargs)


    def get_db_path(self, scrip: str, exchange: str,
                    storage_type: OHLCStorageType):
        exchange = exchange.replace("-", "_").replace(":", "_").replace(" ", "_")
        scrip = scrip.replace("-", "_").replace(":", "_").replace(" ", "_")
        root = os.path.join(self.data_path, self.ProviderName,
                            "historical_data", exchange, scrip)
        os.makedirs(root, exist_ok=True)
        if self.StorageClass == SqliteOHLCStorage:
            if storage_type == OHLCStorageType.PERM:
                return os.path.join(root, f"{scrip}__{exchange}_perm.sqlite")
            elif storage_type == OHLCStorageType.LIVE:
                return os.path.join(root, f"{scrip}__{exchange}_live.sqlite")
            else:
                raise ValueError(f"Cannot find DB for type {storage_type}")
        else:
            raise ValueError(f"Cannot handle storage type {self.StorageClass}")

    def get_storage(self, scrip: str,
                    exchange: str,
                    storage_type: OHLCStorageType):
        db_path = self.get_db_path(scrip, exchange, storage_type)
        return self.StorageClass(db_path)

    def get_data_as_df(self,
                       scrip:str,
                       exchange: str,
                       interval: str,
                       from_date: datetime.datetime,
                       to_date: datetime.datetime,
                       storage_type: OHLCStorageType = OHLCStorageType.PERM) -> pd.DataFrame:
        
        storage = self.get_storage(scrip, exchange, storage_type)
        if storage_type == OHLCStorageType.LIVE:
            conflict_resolution_type = "REPLACE"
        else:
            conflict_resolution_type = "IGNORE"
        data = storage.get(scrip, exchange, from_date, to_date, conflict_resolution_type=conflict_resolution_type)

        data = self.postprocess_data(data, interval)
        self.logger.info(f"Read {len(data)} rows.")
        return data


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
    

class HistoricDataProvider(DataProvider):
    
    def __init__(self,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
    
    @abstractmethod
    def download_historic_data(self,
                               scrip:str,
                               exchange: str,
                               interval: str,
                               from_date: Union[datetime.datetime, str],
                               to_date: Union[datetime.datetime, str]) -> bool:
        pass

    def store_perm_data(self, scrip: str,
                        exchange: str,
                        data: pd.DataFrame):
        storage = self.get_storage(scrip, exchange, storage_type=OHLCStorageType.PERM)
        storage.put(scrip, exchange, data, conflict_resolution_type="IGNORE")

    def download_data_in_batches(self,
                                 scrip: str,
                                 exchange: str,
                                 from_date: Union[datetime.datetime, str],
                                 to_date: Union[datetime.datetime, str]) -> bool:
        subtracting_func = {"days": self.batch_size}
        batch_to_date = to_date
        batch_from_date = batch_to_date - datetime.timedelta(**subtracting_func)
        batch_from_date = max(from_date, batch_from_date)
        self.logger.info(f"Beginning downloading of data in batches for {scrip}/{exchange} between {from_date} and {to_date}...")
        while ((batch_from_date >= from_date or
            (batch_to_date <= to_date and
                batch_to_date >= from_date))):
            self.logger.info(f"Batch {batch_from_date} -- {batch_to_date}")
            data = self.get_data_as_df(scrip=scrip,
                                       exchange=exchange,
                                       interval="1min",
                                       storage_type=OHLCStorageType.PERM,
                                       download_missing_data=True,
                                       from_date=batch_from_date,
                                       to_date=batch_to_date)
            if len(data) == 0:
                break
            if batch_from_date == from_date:
                break
            batch_to_date = batch_from_date
            batch_from_date = batch_from_date - datetime.timedelta(**subtracting_func)
            batch_from_date = max(from_date, batch_from_date)
        return True

    def get_data_as_df(self,
                       scrip:str,
                       exchange: str,
                       interval: str,
                       from_date: datetime.datetime,
                       to_date: datetime.datetime,
                       storage_type: OHLCStorageType = OHLCStorageType.PERM,
                       download_missing_data: bool = False) -> pd.DataFrame:
        
        data = super().get_data_as_df(scrip=scrip,
                                      exchange=exchange,
                                      interval=interval,
                                      from_date=from_date,
                                      to_date=to_date,
                                      storage_type=storage_type)
        # self.logger.info(data)
        if download_missing_data and storage_type == OHLCStorageType.PERM:
            if len(data) > 0:
                days = set(data.index.to_series().apply(lambda x: x.to_pydatetime().replace(hour=0, minute=0, second=0, microsecond=0)))
                start_datetime = from_date.replace(hour=0, minute=0, second=0, microsecond=0)
                while start_datetime < to_date:
                    if start_datetime not in days and not start_datetime.weekday() in [5,6]:
                        self.download_historic_data(scrip=scrip,
                                                exchange=exchange,
                                                interval="1min",
                                                from_date=start_datetime,
                                                to_date=start_datetime + datetime.timedelta(days=1))
                    start_datetime = start_datetime + datetime.timedelta(days=1)
            else:
                self.download_historic_data(scrip=scrip,
                                            exchange=exchange,
                                            interval="1min",
                                            from_date=from_date,
                                            to_date=to_date)
        data = super().get_data_as_df(scrip=scrip,
                                      exchange=exchange,
                                      interval=interval,
                                      from_date=from_date,
                                      to_date=to_date,
                                      storage_type=storage_type)
        return data

class StreamingDataProvider(DataProvider):

    def __init__(self, *args,
                 save_frequency: int = 5,
                 **kwargs):
        self.kill_tick_thread = False
        self.cache = defaultdict(dict)
        self.tick_counter = defaultdict(int)
        self.save_frequency = save_frequency
        super().__init__(*args, **kwargs)

    @abstractmethod
    def start(self, instruments: list, *args, **kwargs):
        pass

    @abstractmethod
    def on_message(self, *args, **kwargs):
        pass

    @abstractmethod
    def on_connect(self, *args, **kwargs):
        pass

    @abstractmethod
    def on_close(self, *args, **kwargs):
        pass

    def on_error(self, *args, **kwargs):
        pass

    def kill(self):
        self.kill_tick_thread = True

    def __save_ticks(self, token):
        df = pd.DataFrame.from_dict(self.cache[token], orient='index')
        df.index = df["date"]
        df.drop(["date"], axis=1, inplace=True)
        df.index = pd.to_datetime(df.index)

        scrip, exchange = get_scrip_and_exchange_from_key(token)
        storage = self.get_storage(scrip, exchange, OHLCStorageType.LIVE)
        storage.put(scrip, exchange, df, conflict_resolution_type="REPLACE")

    def on_tick(self,
                token: str,
                ltp: float,
                ltq: float,
                ltt: datetime.datetime,
                *args,
                **kwargs):
        token = str(token)
        key = ltt.strftime("%Y%m%d %H:%M")
        key = str(key)
        
        if token not in self.cache:
            self.cache[token] = {}

        if key not in self.cache[token]:
            self.cache[token][key] = {"date": key,
                                      "open": ltp,
                                      "high": ltp,
                                      "low": ltp,
                                      "close": ltp,
                                      "volume": ltq,
                                      "oi": 0.}

        if ltp > self.cache[token][key]["high"]:
            self.cache[token][key]["high"] = ltp

        if ltp < self.cache[token][key]["low"]:
            self.cache[token][key]["low"] = ltp
        self.cache[token][key]["close"] = ltp
        self.cache[token][key]["volume"] += ltq
        self.tick_counter[token] += 1
        if self.tick_counter[token] % self.save_frequency == 0:
            self.__save_ticks(token)


class Broker(TradingServiceProvider):

    def __init__(self,
                 *args,
                 **kwargs):

        self.gtt_orders = []
        super().__init__(*args, **kwargs)

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

    # Get state in a form that can be printed as a table.
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
