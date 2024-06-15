from abc import ABC, abstractmethod
from typing import Union, Type
import datetime
import os
import pickle
from threading import Lock
from collections import defaultdict
import http.server
import socketserver
from urllib.parse import urlparse, parse_qs
from typing import Optional
from threading import Lock

import pandas as pd
from tabulate import tabulate

from .logging import LoggerMixin
from .ds import (Order,
                       Position,
                       TransactionType,
                       TradingProduct,
                       OrderType,
                       OrderState,
                       OHLCStorageType)
from .util import (resample_candle_data,
                   get_scrip_and_exchange_from_key,
                   sanitize,
                   new_id)
from .reflection import dynamically_load_class

from .persistence.sqlite.ohlc import SqliteOHLCStorage
from .persistence.ohlc import OHLCStorageMixin
from .persistence.tradebook import TradeBookStorageMixin
from .persistence.sqlite.tradebook import SqliteTradeBookStorage



def nse_commission_func(order: Order, brokerage_percentage: float = 0.03, max_commission: float = 20):
    charges = 0.
    if max_commission > 0:
        brokerage = min((brokerage_percentage / 100) * order.price * order.quantity, max_commission)
    else:
        brokerage = (brokerage_percentage / 100) * order.price * order.quantity
    stt = 0.
    if order.product == TradingProduct.MIS:
        if order.transaction_type == TransactionType.SELL:
            stt = (0.025 / 100) * order.price * order.quantity # STT
    else:
        stt = (0.1 / 100) * order.price * order.quantity # STT
    transaction_charges = (0.00325 / 100) * order.price * order.quantity # Transaction charges NSE
    sebi_charges = (order.price * order.quantity / 10000000) * 10
    stamp_charges = 0.
    if order.transaction_type == TransactionType.BUY:
        stamp_charges = (0.015 / 100) * (order.price * order.quantity / 10000000)
    gst = (18 / 100) * (brokerage + sebi_charges + transaction_charges)
    
    brokerage = round(brokerage, 2)
    stt = round(stt, 2)
    transaction_charges = round(transaction_charges, 2)
    sebi_charges = round(sebi_charges, 2)
    stamp_charges = round(stamp_charges, 2)
    gst = round(gst, 2)
    
    total = round(brokerage + stt + transaction_charges + sebi_charges + stamp_charges + gst, 2)
    print(f"Brokerage: {brokerage} for {order.order_id[:4]} {order.transaction_type}"
          f"| STT: {stt} "
          f"| TransactionCharges: {transaction_charges} "
          f"| SEBICharges: {sebi_charges} "
          f"| Stamp: {stamp_charges} "
          f"| GST: {gst} "
          f"| Total : {total}")
    return total



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

    @property
    def access_token_filepath(self):
        os.makedirs(os.path.join(self.auth_cache_filepath, self.ProviderName), exist_ok=True)
        return os.path.join(self.auth_cache_filepath, self.ProviderName, "access_token.json")

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
                 StorageClass: Type[OHLCStorageMixin] = SqliteOHLCStorage,
                 **kwargs):
        self.data_path = data_path
        self.StorageClass = StorageClass
        super().__init__(*args, **kwargs)


    def get_db_path(self, scrip: str, exchange: str,
                    storage_type: OHLCStorageType):
        exchange = sanitize(exchange)
        scrip = sanitize(scrip)
        root = os.path.join(self.data_path, self.ProviderName,
                            "historical_data", exchange, scrip)
        os.makedirs(root, exist_ok=True)
        if self.StorageClass == SqliteOHLCStorage:
            if storage_type == OHLCStorageType.PERM:
                return os.path.join(root, f"{scrip}__{exchange}_perm.sqlite")
            elif storage_type == OHLCStorageType.LIVE:
                return os.path.join(root, f"{scrip}__{exchange}_live.sqlite")
            else:
                raise ValueError(f"Cannot find DB for type {storage_type} [{type(storage_type)}]")
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
        data = storage.get(scrip, exchange, from_date, to_date,
                           conflict_resolution_type=conflict_resolution_type)

        data = self.postprocess_data(data, interval)
        self.logger.debug(f"Read {len(data)} rows.")
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
        data.index = pd.to_datetime(data.index).tz_localize(None)
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
                               to_date: Union[datetime.datetime, str],
                               finegrained: bool = False) -> bool:
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
                self.logger.info(f"No more data found. breaking")
                break
            batch_to_date = batch_from_date
            batch_from_date = batch_from_date - datetime.timedelta(**subtracting_func)
        # print(batch_from_date, batch_to_date, from_date, to_date)
        return True

    def get_data_as_df(self,
                       scrip:str,
                       exchange: str,
                       interval: str,
                       from_date: datetime.datetime,
                       to_date: datetime.datetime,
                       storage_type: OHLCStorageType = OHLCStorageType.PERM,
                       download_missing_data: bool = False,
                       finegrained_scan: bool = False) -> pd.DataFrame:

        data = super().get_data_as_df(scrip=scrip,
                                      exchange=exchange,
                                      interval=interval,
                                      from_date=from_date,
                                      to_date=to_date,
                                      storage_type=storage_type)

        if download_missing_data and storage_type == OHLCStorageType.PERM:
            if len(data) > 0:
                ticks = set([item.to_pydatetime()
                            for item in data.index.to_series().apply(lambda x: x.to_pydatetime())])
                if not finegrained_scan:
                    ticks = set([item.replace(hour=0, minute=0, second=0, microsecond=0) for item in ticks])
                    start_datetime = from_date.replace(hour=0, minute=0, second=0, microsecond=0)
                else:
                    ticks = set([item.replace(second=0, microsecond=0) for item in ticks])
                    start_datetime = from_date.replace(second=0, microsecond=0)
                while start_datetime < to_date:
                    if start_datetime not in ticks and not start_datetime.weekday() in [5,6]:
                        self.logger.info(f"Could not find data for {start_datetime}; "
                                         f"Starting download of data from provider for "
                                         f"{scrip}/{exchange} for "
                                         f"{start_datetime}")
                        self.download_historic_data(scrip=scrip,
                                                    exchange=exchange,
                                                    interval="1min",
                                                    from_date=start_datetime,
                                                    to_date=start_datetime,
                                                    finegrained=finegrained_scan)
                    if not finegrained_scan:
                        start_datetime = start_datetime + datetime.timedelta(days=1)
                    else:
                        start_datetime = start_datetime + datetime.timedelta(minutes=1)
            else:
                self.logger.info(f"No data found.Starting download of data from provider for "
                                 f"{scrip}/{exchange} from {from_date} to {to_date}")
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
                 clear_live_data_cache: bool = True,
                 market_start_hour: int = 9,
                 market_start_minute: int = 15,
                 **kwargs):
        self.kill_tick_thread = False
        self.clear_live_data_cache = clear_live_data_cache
        self.cache = defaultdict(dict)
        self.tick_counter = defaultdict(int)
        self.save_frequency = save_frequency
        self.market_start_hour = market_start_hour
        self.market_start_minute = market_start_minute
        super().__init__(*args, **kwargs)

    def clear_live_storage(self, instruments: list):
        for instrument in instruments:
            storage = self.get_storage(instrument["scrip"],
                                       instrument["exchange"],
                                       OHLCStorageType.LIVE)
            self.logger.info(f"Clearing live data for {instrument['scrip']}"
                             f"/{instrument['exchange']}")
            storage.clear_data(instrument["scrip"], instrument["exchange"])

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
        if (ltt.hour < self.market_start_hour or
            (ltt.hour == self.market_start_hour and ltt.minute < self.market_start_minute)):
            self.logger.warn(f"Found data from a datetime that's before market start {ltt}")
            return
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

    ProviderName = "unknown"

    def __init__(self,
                 audit_records_path: str,
                 *args,
                 TradingBookStorageClass: Type[TradeBookStorageMixin] = SqliteTradeBookStorage,
                 strategy: Optional[str] = None,
                 run_name: Optional[str] = None,
                 thread_id: str = "1",
                 disable_state_persistence: bool = False,
                 commission_func: Optional[callable] = None,
                 **kwargs):
        LoggerMixin.__init__(self, *args, **kwargs)
        if isinstance(TradingBookStorageClass, str):
            TradingBookStorageClass = dynamically_load_class(TradingBookStorageClass)
        self.TradingBookStorageClass = TradingBookStorageClass
        self.audit_records_path = audit_records_path
        self.strategy = strategy
        self.run_name = run_name
        self.run_id = new_id()
        self.thread_id = thread_id
        self.order_state_lock = Lock()
        self.position_state_lock = Lock()
        self.gtt_state_lock = Lock()
        self.state_file_lock = Lock()
        self.disable_state_persistence = disable_state_persistence
        self.gtt_orders = []
        self.trade_pnl = {}
        if commission_func is None:
            commission_func = nse_commission_func
        self.commission_func = commission_func
        super().__init__(*args, **kwargs)
        self.load_state()

    def load_state(self):
        with self.state_file_lock:
            self.state_filepath = os.path.join(self.audit_records_path,
                                            f"state_{self.__class__.__name__}_"
                                            f"{self.thread_id}.pickle")
            self.logger.debug(f"Searching for state in {self.state_filepath}")
            if os.path.exists(self.state_filepath):
                self.logger.info(f"Loading broker state from {self.state_filepath}")
                with open(self.state_filepath, 'rb') as fid:
                    state = pickle.load(fid)
                    self.gtt_orders = state["gtt_orders"]
                    if state["extra"] is not None:
                        for k, v in state["extra"].items():
                            setattr(self, k, v)
            else:
                self.logger.info(f"No prior broker state found.")

    def save_state(self):
        if not self.disable_state_persistence:
            self.logger.debug(f"Saving state to {self.state_filepath}")
            with open(self.state_filepath, 'wb') as fid:
                state = self.get_state()
                pickle.dump(state, fid)

    def clear_tradebooks(self, scrip: str, exchange: str):
        if (self.strategy is not None and self.run_name is not None):
            self.get_tradebook_storage().clear_run(self.strategy, self.run_name,
                                                   scrip=scrip, exchange=exchange)

    @abstractmethod
    def get_state(self) -> object:
        pass

    def get_tradebook_db_path(self):
        root = os.path.join(self.audit_records_path, self.ProviderName,
                            "trade_book")
        os.makedirs(root, exist_ok=True)
        if self.TradingBookStorageClass == SqliteTradeBookStorage:
            return os.path.join(root, f"tradebook-{self.thread_id}.sqlite")
        else:
            return os.path.join(root, f"tradebook-{self.thread_id}")

    def get_tradebook_storage(self) -> TradeBookStorageMixin:
        if not hasattr(self, "tradebook_storage"):
            self.logger.debug("Connecting to new Tradebook storage")
            db_path = self.get_tradebook_db_path()
            self.tradebook_storage = self.TradingBookStorageClass(db_path)
        return self.tradebook_storage

    def cancel_invalid_child_orders(self):
        state_changed = False
        for order in self.get_orders(refresh_cache=False):
            if order.state != OrderState.PENDING:
                if order.parent_order_id is not None:
                    #self.logger.info(f"Searching for siblings of "
                    #                 f"{order.order_id} (parent={order.parent_order_id})")
                    for other_order in self.get_orders(refresh_cache=False):
                        if (other_order.parent_order_id == order.parent_order_id and
                            other_order.order_id != order.order_id and
                            other_order.state == OrderState.PENDING):
                            state_changed = True
                            self.logger.info(f"Cancelling order {other_order.order_id}/"
                                            f"{other_order.scrip}/{other_order.exchange}/"
                                            f"{other_order.transaction_type}/{other_order.order_type}"
                                            f"{','.join(other_order.tags)} due OCO (Sibling of {order.order_id})")
                            self.cancel_order(other_order, refresh_cache=False)
                            self.delete_gtt_orders_for(other_order)
        if state_changed:
            self.get_orders(refresh_cache=True)

    def cancel_invalid_group_orders(self):
        for order in self.get_orders(refresh_cache=False):
            state_changed = False
            if (order.group_id is not None
                and order.state != OrderState.PENDING):
                #self.logger.info(f"Searching for group members of "
                #                 f"{order.order_id} (group_id={order.group_id})")
                for other_order in self.get_orders(refresh_cache=False):
                    if (other_order.group_id == order.group_id
                        and other_order.order_id != order.order_id
                        and other_order.state == OrderState.PENDING):
                        state_changed = True
                        self.logger.info(f"Cancelling order {other_order.order_id[:4]}/"
                                         f"{other_order.scrip}/{other_order.exchange}/"
                                         f"{other_order.transaction_type}/{other_order.order_type}"
                                         f"{','.join(other_order.tags)} due OCO (Group of {order.group_id})")
                        self.cancel_order(other_order, refresh_cache=False)
                        self.delete_gtt_orders_for(other_order)
            if state_changed:
                self.get_orders(refresh_cache=True)

    # Get state in a form that can be printed as a table.
    def get_orders_as_table(self) -> (list[list], list):
        printable_orders = []
        for order in self.get_orders():
            if order.state == OrderState.PENDING:
                printable_orders.append(["R",
                                         order.order_id[:4],
                                         order.parent_order_id[:4] if order.parent_order_id is not None else "",
                                         order.group_id[:4] if order.group_id is not None else "",
                                         order.scrip,
                                         order.exchange,
                                         order.transaction_type,
                                         order.quantity,
                                         order.order_type,
                                         order.limit_price,
                                         ", ".join(order.tags)])
        for from_order, to_order in self.get_gtt_orders():
            printable_orders.append(["GTT",
                                     to_order.order_id[:4],
                                     from_order.order_id[:4],
                                     to_order.group_id[:4] if to_order.group_id is not None else "",
                                     to_order.scrip,
                                     to_order.exchange,
                                     to_order.transaction_type,
                                     to_order.quantity,
                                     to_order.order_type,
                                     to_order.limit_price,
                                     ", ".join(to_order.tags)])
        headers = ["Typ", "id", "parent","group_id", "scrip", "exchange",
                   "buy/sell", "qty", "order_type", "limit_price", "reason"]
        print(tabulate(printable_orders, headers=headers, tablefmt="double_outline"))
        return printable_orders, headers

    def get_positions_as_table(self) -> (list[list], list):
        printable_positions = []
        # print(self.positions)
        for position in self.get_positions():
            printable_positions.append([position.timestamp,
                                        position.scrip,
                                        position.exchange,
                                        position.stats["net_quantity"],
                                        position.average_price,
                                        position.last_price,
                                        position.pnl,
                                        position.charges])
        headers = ["time", "scrip", "exchange", "qty", "avgP", "LTP", "PnL", "Comm"]
        print(tabulate(printable_positions, headers=headers, tablefmt="double_outline"))
        return printable_positions, headers

    @abstractmethod
    def get_orders(self, refresh_cache: bool = True) -> list[Order]:
        pass

    @abstractmethod
    def place_order(self,
                    order: Order,
                    refresh_cache: bool = True) -> Order:
        pass

    def order_callback(self,
                       *args, **kwargs):
        raise NotImplementedError("Order Callback")

    @abstractmethod
    def update_order(self, order: Order,
                     local_update: bool = False,
                     refresh_cache: bool = True) -> Order:
        pass
    """
    Deprecated....
    def cancel_pending_orders(self,
                              scrip: Optional[str] = None,
                              exchange: Optional[str] = None,
                              refresh_cache: bool = True):
        for order in self.get_orders(refresh_cache=refresh_cache):
            if scrip is not None and exchange is not None:
                if scrip != order.scrip or exchange != order.exchange:
                    continue
            if order.state == OrderState.PENDING:
                self.cancel_order(order, refresh_cache=False)
                self.delete_gtt_orders_for(order)
        self.get_orders(refresh_cache=refresh_cache)
        self.cancel_invalid_child_orders()
        self.cancel_invalid_group_orders()
        self.get_orders(refresh_cache=refresh_cache)
    """

    @abstractmethod
    def cancel_order(self, order: Order,
                     refresh_cache: bool = True) -> Order:
        pass

    @abstractmethod
    def get_positions(self,
                      refresh_cache: bool = True) -> list[Position]:
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
                            tags: Optional[list] = None,
                            strategy: str = None,
                            run_name: str = None,
                            run_id: str = None) -> Order:
        if run_id is None:
            run_id = self.run_id
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
        if strategy is not None and run_name is not None:
            storage = self.get_tradebook_storage()
            storage.store_order_execution(strategy=strategy,
                                          run_name=run_name,
                                          run_id=run_id,
                                          date=self.current_datetime(),
                                          order=order,
                                          event="OrderCreated")
        return order

    def place_gtt_order(self,
                        entry_order: Order,
                        other_order: Order) -> (Order, Order):
        with self.gtt_state_lock:
            self.gtt_orders.append((entry_order, other_order))
        self.save_state()
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
        with self.gtt_state_lock:
            for ii, (o1, o2) in enumerate(self.gtt_orders):
                if (o1.order_id == entry_order.order_id
                    and o2.order_id == other_order.order_id):
                    self.gtt_orders[ii] == (entry_order, other_order)
        self.save_state()
        return entry_order, other_order

    def delete_gtt_orders_for(self, order: Order):
        new_gtt_orders = []
        with self.gtt_state_lock:
            for o1, o2 in self.get_gtt_orders():
                if o1.order_id == order.order_id:
                    continue
                new_gtt_orders.append((o1, o2))
            self.gtt_orders = new_gtt_orders
        self.save_state()

    def clear_gtt_orders(self):
        with self.gtt_state_lock:
            self.gtt_orders =[]
        self.save_state()

    def gtt_order_callback(self,
                           refresh_cache: bool = True) -> bool:
        new_gtt_orders = []
        gtt_state_changed = False
        self.get_orders(refresh_cache=refresh_cache)
        with self.gtt_state_lock:
            for entry_order, other_order in self.gtt_orders:
                print(entry_order.state, other_order.state)
                if (entry_order.state == OrderState.COMPLETED
                    and other_order.state == OrderState.PENDING):
                    if entry_order.product == TradingProduct.MIS:
                        self.logger.info(f"Placing MIS GTT Order for {entry_order.order_id} {other_order.tags}")
                        self.place_order(other_order, refresh_cache=False)
                        # print(other_order)
                        gtt_state_changed = True
                        continue
                    elif entry_order.product in [TradingProduct.NRML, TradingProduct.CNC]: 
                        # This means, there is some order placed for long term. 
                        # Check positions and sufficient position exists and the pending 
                        # order is from the previous work day, place gtt orders.
                        
                        self.place_order(other_order, refresh_cache=False)
                        continue
                elif (entry_order.state == OrderState.CANCELLED or entry_order.state == OrderState.REJECTED):
                    gtt_state_changed = True
                    continue
                new_gtt_orders.append((entry_order, other_order))
            self.gtt_orders = new_gtt_orders
        self.get_orders(refresh_cache=refresh_cache)
        self.cancel_invalid_child_orders()
        self.cancel_invalid_group_orders()
        self.get_orders(refresh_cache=refresh_cache)
        self.save_state()
        return gtt_state_changed

    def update_gtt_orders_for(self, order: Order):
        with self.gtt_state_lock:
            for ii, (from_order, to_order) in enumerate(self.gtt_orders):
                print(ii, from_order.order_id, to_order.order_id, order.order_id)
                if from_order.order_id == order.order_id:
                    self.gtt_orders[ii] = (order, to_order)


    def start_order_change_streamer(self):
        pass
