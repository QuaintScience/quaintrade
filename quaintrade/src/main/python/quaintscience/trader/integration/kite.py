import datetime
import json
import pytz
import os
from threading import Thread
from functools import cache

from typing import Union, Optional

from kiteconnect import KiteConnect, KiteTicker
from kiteconnect.exceptions import InputException
import pandas as pd
import datetime
import time
import traceback

from ..core.ds import Order, OrderType, TradingProduct, TransactionType, Position, OHLCStorageType, OrderState
from ..core.roles import HistoricDataProvider, AuthenticatorMixin, Broker, StreamingDataProvider
from ..core.util import today_timestamp, hash_dict, datestring_to_datetime, get_key_from_scrip_and_exchange


class KiteBaseMixin(AuthenticatorMixin):

    ProviderName = "kite"

    def __init__(self, *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

    def login(self):
        self.kite = KiteConnect(api_key=self.auth_credentials["API_KEY"])
        self.auth_state = {"state": "Start Login"}
        if self.reset_auth_cache:
            if os.path.exists(self.access_token_filepath):
                os.remove(self.access_token_filepath)

        if os.path.exists(self.access_token_filepath):
            self.logger.info(f"Loading access token from {self.access_token_filepath}")
            with open(self.access_token_filepath, 'r', encoding='utf-8') as fid:
                try:
                    self.auth_state = json.load(fid)
                    self.logger.info(f"Found auth state {self.auth_state}")
                    self.kite.set_access_token(self.auth_state["access_token"])
                    self.kite.instruments()
                    self.init()
                except Exception:
                    self.logger.info(self.kite.login_url())
                    response = self.listen_to_login_callback()
                    self.finish_login(response["query_params"]["request_token"][0])
            return None
        self.logger.info(self.kite.login_url())
        response = self.listen_to_login_callback()
        self.finish_login(response["query_params"]["request_token"][0])

    def finish_login(self, request_token,):
        try:
            self.auth_state = self.kite.generate_session(request_token, self.auth_credentials["API_SECRET"])
            self.kite.set_access_token(self.auth_state["access_token"])
            self.auth_state["state"] = "Logged in"
            self.auth_state["login_time"] = self.auth_state["login_time"].strftime("%Y-%m-%d %H:%M:%S")
            with open(self.access_token_filepath, 'w', encoding='utf-8') as fid:
                json.dump(self.auth_state, fid)
            self.init()
        except Exception:
            self.auth_state["state"] = "Login Error"
            self.auth_state["traceback"] = traceback.format_exc()

    def init(self):
        self.__load_instrument_token_mapper()

    def __load_instrument_token_mapper(self, force_refresh: bool = False):
        today = today_timestamp()
        filepath = os.path.join(f"{self.auth_cache_filepath}", self.ProviderName, f"instruments-{today}.csv")
        if not os.path.exists(filepath) or force_refresh:
            instruments = pd.DataFrame(self.kite.instruments())
            instruments.to_csv(filepath, index=False)
        self.instruments = pd.read_csv(filepath)

    def kite_timestamp_to_datetime(self, d):
        return datetime.datetime.strptime(d, "%Y-%m-%d %H:%M:%S")

    @hash_dict
    @cache
    def get_instrument_object(self, instruments):
        result = []
        instruments_lst = instruments
        if isinstance(instruments, dict):
            instruments_lst = [instruments]
        for instrument_dct in instruments_lst:
            scrip = instrument_dct["scrip"]
            exchange = instrument_dct["exchange"]
            instrument = self.instruments[((self.instruments["tradingsymbol"] == scrip) &
                                       (self.instruments["exchange"] == exchange))]
            if len(instrument) > 1:
                raise ValueError(f"Ambiguous symbol {scrip} in "
                                f"exchange {exchange}\n {instrument.to_dict()}")
            elif len(instrument) == 0:
                raise ValueError(f"No date found for symbol {scrip} in exchange {exchange}")
            result.append(instrument.iloc[0].to_dict())
        if isinstance(instruments, dict):
            return result[0]
        return result


class KiteStreamingMixin():
    
    def __init__(self, *args,
                 on_message: Optional[callable] = None,
                 on_connect: Optional[callable] = None,
                 on_close: Optional[callable] = None,
                 on_order_update: Optional[callable] = None,
                 on_error: Optional[callable] = None,
                 **kwargs):
        if on_message is None and on_order_update is None:
            raise ValueError("Streaming cannot work when both on_message and on_orders is None.")
        self.on_message_callable = on_message
        self.on_connect_callable = on_connect
        self.on_close_callable = on_close
        self.on_order_update_callable = on_order_update
        self.on_error_callable = on_error

    def start_streamer(self):
        self.kws = KiteTicker(self.auth_credentials["API_KEY"],
                              self.auth_state["access_token"])
        if self.on_message_callable is not None:
            self.kws.on_ticks = self.on_message_callable
        if self.on_order_update_callable is not None:
            self.kws.on_order_update = self.on_order_update_callable
        if self.on_connect_callable is not None:
            self.kws.on_connect = self.on_connect_callable
        if self.on_close_callable is not None:
            self.kws.on_close = self.on_close_callable
        if self.on_error_callable is not None:
            self.kws.on_error = self.on_error_callable
        self.logger.info("Starting streamer....")
        self.ticker_thread = Thread(target=self.kws.connect,
                                    kwargs={"threaded": True})
        self.ticker_thread.start()

    def on_close(self, ws, code, reason, *args, **kwargs):
        self.logger.info(f"Ticker Websock closed {code} / {reason}.")


class KiteHistoricDataProvider(KiteBaseMixin, HistoricDataProvider):

    ProviderName = "kite"

    def __init__(self,
                 *args,
                 rate_limit_time: float = 0.33,
                 batch_size: int = 59,
                 **kwargs):
        HistoricDataProvider.__init__(self, *args, **kwargs)
        KiteBaseMixin.__init__(self, *args, **kwargs)
        self.rate_limit_time = rate_limit_time
        self.batch_size = batch_size

    def download_historic_data(self,
                               scrip:str,
                               exchange: str,
                               interval: str,
                               from_date: Union[datetime.datetime, str],
                               to_date: Union[datetime.datetime, str]) -> bool:
        if interval == "1min":
            interval = "minute"

        if isinstance(from_date, str):
            from_date = datestring_to_datetime(from_date)
        if isinstance(to_date, str):
            to_date = datestring_to_datetime(to_date)

        req_start_time = time.time()
        instrument = self.get_instrument_object({"scrip": scrip, "exchange": exchange})
        self.logger.info(f"Start fetch data from kite {from_date} to {to_date}")
        data = self.kite.historical_data(instrument["instrument_token"],
                                        interval=interval,
                                        from_date=from_date,
                                        to_date=to_date,
                                        oi=True)
        if len(data) == 0:
            return False
        data = pd.DataFrame(data)
        try:
            data["date"] = data["date"].dt.tz_localize(None)
        except Exception:
            return False
        data.index = data["date"]
        data.drop(["date"], axis=1, inplace=True)

        self.store_perm_data(scrip, exchange, data)
        time_elapsed = time.time() - req_start_time
        self.logger.info(f"Fetching {len(data)} rows of data from kite {from_date} to {to_date} took {time_elapsed:.2f} seconds")
        if time_elapsed < self.rate_limit_time:
            time.sleep(self.rate_limit_time - time_elapsed)
        return True


class KiteBroker(KiteBaseMixin,
                 Broker,
                 KiteStreamingMixin):

    ProviderName = "kite"

    def __init__(self,
                 *args,
                 **kwargs):
        self.orders_cache = []
        self.positions_cache = []
        self.rate_limit_time: float = 0.33
        Broker.__init__(self, *args, **kwargs)
        KiteBaseMixin.__init__(self, *args, **kwargs)
        kwargs["on_order_update"] = self.order_callback
        kwargs["on_error"] = self.error_callback
        KiteStreamingMixin.__init__(self, *args, **kwargs)

    def get_state(self) -> dict:
        return {"gtt_orders": self.gtt_orders,
                "extra": {"orders_cache": self.orders_cache,
                          "positions_cache": self.positions_cache}}

    # Order management

    """
    def cancel_pending_orders(self, refresh_cache: bool = True):
        for order in self.get_orders(refresh_cache=refresh_cache):
            if order.state == OrderState.PENDING:
                self.cancel_order(order, refresh_cache=refresh_cache)
        self.get_orders(refresh_cache=refresh_cache)
    """

    def cancel_order(self, order: Order, refresh_cache: bool = True) -> Order:
        self.logger.info(f"Cancelling order with order_id {order.order_id}...")
        for existing_order in self.get_orders(refresh_cache=refresh_cache):
            if existing_order.order_id == order.order_id:
                if existing_order.state == OrderState.PENDING:
                    try:
                        self.kite.cancel_order(variety=self.kite.VARIETY_REGULAR,
                                            order_id=order.order_id)
                        time.sleep(self.rate_limit_time)
                    except InputException:
                        self.logger.warn(f"Could not delete order {order.order_id}")
                        storage = self.get_tradebook_storage()
                        storage.store_order_execution(self.strategy, self.run_name,
                                                        run_id=self.run_id,
                                                        date=self.current_datetime(),
                                                        order=order, event="OrderCancelledFailed")
                        continue
                    self.get_orders(refresh_cache=refresh_cache)
                    storage = self.get_tradebook_storage()
                    storage.store_order_execution(self.strategy, self.run_name,
                                                  run_id=self.run_id,
                                                date=self.current_datetime(),
                                                order=order, event="OrderCancelled")
                else:
                    self.logger.info(f"Did not cancel order as it's state is {existing_order.state} {type(order.state)}")
                break

    def update_order(self, order: Order,
                     local_update: bool = False,
                     refresh_cache: bool = True) -> Order:
        self.logger.info(f"Attempting update of order with order_id {order.order_id}...")
        for existing_order in self.get_orders(refresh_cache=refresh_cache):
            if existing_order.order_id == order.order_id:
                if local_update:
                    existing_order.tags = order.tags
                    existing_order.parent_order_id = order.parent_order_id
                    existing_order.group_id = order.group_id
                
                if existing_order.state == OrderState.PENDING and not local_update:
                    self.logger.info(f"Found order with order_id {order.order_id} for updation...")
                    order_id = self.kite.modify_order(variety=self.kite.VARIETY_REGULAR,
                                                      order_id=order.order_id,
                                                      quantity=int(order.quantity),
                                                      trigger_price=round(order.trigger_price, 1),
                                                      price=round(order.price, 1))
                    time.sleep(self.rate_limit_time)
                    if order_id != order.order_id:
                        self.logger.warn(f"Order ID changed from {order.order_id} to {order_id} after update.")
                        order.order_id = order_id
                        with self.order_state_lock:
                            self.orders_cache.append(order)
                    self.get_orders(refresh_cache=refresh_cache)
                    return order
            else:
                self.logger.info(f"Did not update order as order state is {order.state}")
        self.logger.info(f"Could not find order with order_id {order.order_id} for updation...")

    def __translate_order_type(self, order: Order):
        if order.order_type == OrderType.SL_LIMIT:
            return self.kite.ORDER_TYPE_SL
        elif order.order_type == OrderType.MARKET:
            return self.kite.ORDER_TYPE_MARKET
        elif order.order_type == OrderType.LIMIT:
            return self.kite.ORDER_TYPE_LIMIT
        elif order.order_type == OrderType.SL_MARKET:
            return self.kite.ORDER_TYPE_SLM

    def __translate_transaction_type(self, order: Order):
        if order.transaction_type == TransactionType.BUY:
            return self.kite.TRANSACTION_TYPE_BUY
        elif order.transaction_type == TransactionType.SELL:
            return self.kite.TRANSACTION_TYPE_SELL
    
    def __translate_product(self, order: Order):
        if order.product == TradingProduct.MIS:
            return self.kite.PRODUCT_MIS
        elif order.price == TradingProduct.CNC:
            return self.kite.PRODUCT_CNC
        elif order.price == TradingProduct.NRML:
            return self.kite.PRODUCT_NRML

    def place_order(self, order: Order, refresh_cache: bool = True) -> Order:
        order_kwargs = {"tradingsymbol": order.scrip,
                        "exchange": order.exchange,
                        "transaction_type": self.__translate_transaction_type(order),
                        "quantity": int(order.quantity),
                        "variety": self.kite.VARIETY_REGULAR,
                        "order_type": self.__translate_order_type(order),
                        "product": self.__translate_product(order),
                        "validity": order.validity}
        if order.order_type in [OrderType.LIMIT, OrderType.SL_LIMIT]:
            order_kwargs["price"] = round(order.limit_price, 1)
        if order.order_type in [OrderType.SL_LIMIT, OrderType.SL_MARKET]:
            order_kwargs["trigger_price"] = round(order.trigger_price, 1)
        self.logger.info(f"KITE: {order_kwargs}")
        try:
            order_id = self.kite.place_order(**order_kwargs)
        except InputException:
            traceback.print_exc()
            return None
        order.order_id = order_id
        with self.order_state_lock:
            self.orders_cache.append(order)
        self.get_orders(refresh_cache=refresh_cache)
        self.logger.info(f"Placed order {order.transaction_type} "
                         f"with order_id {order.order_id} for "
                         f"{order.scrip} / {order.exchange} "
                         f"qty={order.quantity} @ {order.limit_price} "
                         f"of type {order.order_type} [tags={order.tags}]")
        return order

    def order_callback(self, ws, message):
        self.logger.info(f"Received order update {message}")
        # self.get_orders(refresh_cache=True) # Commented as gtt_order_callback does this anyway.
        # self.__update_order_in_cache(message) # Locks order cache
        # self.__update_gtt_orders_using_dct(message) # Locks gtt
        self.gtt_order_callback(refresh_cache=True) # Locks order cache intermittently and locks gtt

    def error_callback(self, *args, **kwargs):
        self.gtt_order_callback(refresh_cache=True) # Locks order cache intermittently and locks gtt

    def get_positions(self, refresh_cache: bool = True) -> list[Position]:
        if refresh_cache:
            positions = self.kite.positions()
            holdings = self.kite.holdings()
            positions["day"].extend(holdings)
        else:
            positions = {"day": [], "net": []}
        for position in positions["day"]:
            found_position_in_cache = False
            with self.position_state_lock:
                for existing_position in self.positions_cache:
                    # print(existing_position, position)
                    if (existing_position.scrip == position["tradingsymbol"]
                        and existing_position.exchange == position["exchange"]
                        and existing_position.product == self.__reverse_translate_product(position["product"])):
                        found_position_in_cache = True
                        existing_position.quantity = position["quantity"]
                        existing_position.last_price = position["last_price"]
                        existing_position.pnl = position["pnl"]
                        existing_position.average_price = position["average_price"]
                        existing_position.timestamp = self.current_datetime()
                        break
                if not found_position_in_cache:
                    new_position = Position(scrip_id=position["instrument_token"],
                                            scrip=position["tradingsymbol"],
                                            exchange=position["exchange"],
                                            exchange_id=position["exchange"],
                                            product=self.__reverse_translate_product(position["product"]),
                                            last_price=position["last_price"],
                                            pnl=position["pnl"],
                                            quantity=position["quantity"],
                                            timestamp=self.current_datetime(),
                                            average_price=position["average_price"])
                    self.positions_cache.append(new_position)
        self.save_state()
        return self.positions_cache

    def __reverse_translate_order_type(self, order_type: str):
        if order_type == self.kite.ORDER_TYPE_LIMIT:
            return OrderType.LIMIT
        elif order_type == self.kite.ORDER_TYPE_MARKET:
            return OrderType.MARKET
        elif order_type == self.kite.ORDER_TYPE_SL:
            return OrderType.SL_LIMIT
        elif order_type == self.kite.ORDER_TYPE_SLM:
            return OrderType.SL_MARKET
        else:
            raise ValueError(f"Unknown order type {order_type}")
    
    def __reverse_translate_product(self, product: str):
        if product == self.kite.PRODUCT_CNC:
            return TradingProduct.CNC
        elif product == self.kite.PRODUCT_MIS:
            return TradingProduct.MIS
        elif product == self.kite.PRODUCT_NRML:
            return TradingProduct.NRML
        else:
            raise ValueError(f"Unknown product {product}")

    def __reverse_translate_transaction_type(self, transaction_type: str):
        if transaction_type == self.kite.TRANSACTION_TYPE_BUY:
            return TransactionType.BUY
        elif transaction_type == self.kite.TRANSACTION_TYPE_SELL:
            return TransactionType.SELL
        else:
            raise ValueError(f"Unknown transaction type {transaction_type}")

    def __reverse_translate_order_state(self, order_state: str):
        if order_state == self.kite.STATUS_CANCELLED:
            return OrderState.CANCELLED
        elif order_state == self.kite.STATUS_COMPLETE:
            return OrderState.COMPLETED
        elif order_state == self.kite.STATUS_REJECTED:
            return OrderState.REJECTED
        elif order_state == "TRIGGER PENDING":
            return OrderState.PENDING
        elif order_state == "OPEN":
            return OrderState.PENDING
        elif order_state == "OPEN PENDING":
            return OrderState.PENDING
        elif order_state == "VALIDATION PENDING":
            return OrderState.PENDING
        else:
            raise ValueError(f"Unknown Order State {order_state}")

    def __update_order_from_dct(self, cached_order: Order, order: dict):
        cached_order.quantity = order["quantity"]
        cached_order.trigger_price = order["trigger_price"]
        cached_order.limit_price = order["price"]
        cached_order.filled_quantity = order["filled_quantity"]
        cached_order.pending_quantity = order["pending_quantity"]
        cached_order.cancelled_quantity = order["cancelled_quantity"]
        cached_order.state = self.__reverse_translate_order_state(order["status"])
        cached_order.raw_dict = order

    def __update_order_in_cache(self,
                                order: dict):

        found_in_cache = False
        with self.order_state_lock:
            for cached_order in self.orders_cache:
                if order["order_id"] == cached_order.order_id:
                    self.logger.debug(f"Updated cached order {order['order_id']}")
                    self.__update_order_from_dct(cached_order=cached_order, 
                                                order=order)
                    found_in_cache = True
                    break
            if not found_in_cache:
                self.logger.debug(f"Creating new order in cache for {order['order_id']}")
                new_order = Order(order_id=order["order_id"],
                                    exchange_id=order["exchange"],
                                    scrip=order["tradingsymbol"],
                                    scrip_id=order["instrument_token"],
                                    exchange=order["exchange"],
                                    transaction_type=self.__reverse_translate_transaction_type(order["transaction_type"]),
                                    raw_dict=order,
                                    state=self.__reverse_translate_order_state(order["status"]),
                                    timestamp=order["order_timestamp"],
                                    order_type = self.__reverse_translate_order_type(order["order_type"]),
                                    product = self.__reverse_translate_product(order["product"]),
                                    quantity = order["quantity"],
                                    trigger_price = order["trigger_price"],
                                    limit_price = order["price"],
                                    filled_quantity = order["filled_quantity"],
                                    pending_quantity = order["pending_quantity"],
                                    cancelled_quantity = order["cancelled_quantity"])
                self.orders_cache.append(new_order)

    def get_orders(self, refresh_cache=True) -> list[Order]:

        if refresh_cache:
            orders = self.kite.orders()
            time.sleep(self.rate_limit_time)
        else:
            orders = []
        for order in orders:
            self.__update_order_in_cache(order)
        if refresh_cache:
            for order in orders:
                self.__update_gtt_orders_using_dct(order)
            self.cancel_invalid_child_orders()
            self.cancel_invalid_group_orders()
            self.save_state()
        return self.orders_cache

    def __update_gtt_orders_using_dct(self, order: dict):
        with self.gtt_state_lock:
            for from_order, _ in self.gtt_orders:
                if from_order.order_id == order["order_id"]:
                    self.__update_order_from_dct(from_order, order)

    def start_order_change_streamer(self):
        self.start_streamer()


class KiteStreamingDataProvider(KiteBaseMixin, StreamingDataProvider, KiteStreamingMixin):

    ProviderName = "kite"

    def __init__(self, *args, **kwargs):
        StreamingDataProvider.__init__(self, *args, **kwargs)
        KiteBaseMixin.__init__(self, *args, **kwargs)
        kwargs["on_message"] = self.on_message
        KiteStreamingMixin.__init__(self, *args, **kwargs)

    @cache
    def __get_readable_string(self, instrument_token):
        data = self.instruments[self.instruments["instrument_token"] == instrument_token]
        if len(data) == 0:
            raise ValueError(f"Could not find details of instrument token {instrument_token}")
        if len(data) > 1:
            raise ValueError(f"Unambiguous instrument token {instrument_token} = {data}")
        return get_key_from_scrip_and_exchange(data.iloc[0]["tradingsymbol"],
                                       data.iloc[0]["exchange"])

    def on_message(self, ws, ticks, *args, **kwargs):
        if self.kill_tick_thread:
            self.kill_tick_thread = False
            raise KeyError("Killed Tick Thread!")
        for tick in ticks:
            self.logger.debug(f"Received tick {tick}")
            ltp = tick["last_price"]
            ltq = tick.get("last_quantity", 0)
            ltt = tick.get("last_trade_time", datetime.datetime.now(pytz.timezone('Asia/Kolkata')))
            token = tick["instrument_token"]
            token = self.__get_readable_string(tick["instrument_token"])
            self.on_tick(token, ltp, ltq, ltt, *args, **kwargs)

    def on_connect(self, ws,
                   response,
                   *args, **kwargs):
        self.logger.debug(f"Ticker Websock connected {response}.")
        self.logger.debug(f"Subscribing to {self.ticker_instruments}")
        tokens = [instrument["instrument_token"] for instrument in self.ticker_instruments]
        self.kws.subscribe(tokens)
        self.kws.set_mode(KiteTicker.MODE_QUOTE, tokens)
    
    def start_ticker(self, instruments: list[str], *args, **kwargs):
        instruments = self.get_instrument_object(instruments)
        if isinstance(instruments, dict):
            instruments = [instruments]
        self.ticker_instruments = instruments
        self.start_streamer()
