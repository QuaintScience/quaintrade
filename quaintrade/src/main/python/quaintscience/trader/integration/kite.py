import datetime
import copy
import json
import pytz
from collections import defaultdict
import os
from threading import Thread

from typing import Union

from kiteconnect import KiteConnect, KiteTicker
import pandas as pd
import datetime
import time
import traceback

from ..core.ds import Order, OrderType, TradingProduct, TransactionType
from .common import TradeManager





class KiteManager(TradeManager):

    RATE_LIMIT_TIME=0.33
    BATCH_SIZE = 60

    def __init__(self,
                 *args,
                 **kwargs):
        self.kite = None
        self.kws = None
        self.ticker_thread = None
        super().__init__(*args, **kwargs)


    @property
    def access_token_filepath(self):
        return os.path.join(self.cache_path, "access_token.cache")

    def start_login(self) -> str:
        self.kite = KiteConnect(api_key=self.user_credentials["API_KEY"])
        self.auth_state = {"state": "Start Login"}
        
        if os.path.exists(self.access_token_filepath):
            print(self.access_token_filepath)
            with open(self.access_token_filepath, 'r', encoding='utf-8') as fid:
                try:
                    self.auth_state = json.load(fid)
                    self.logger.info(f"Found auth state {self.auth_state}")
                    self.kite.set_access_token(self.auth_state["access_token"])
                    self.kite.instruments()
                    self.init()
                except Exception:
                    print(self.kite.login_url())
                    response = self.listen_to_login_callback()
                    self.finish_login(response["request_token"])
            return None
        print(self.kite.login_url())
        response = self.listen_to_login_callback()
        self.finish_login(response["query_params"]["request_token"][0])

    def finish_login(self, request_token, *args, **kwargs):
        try:
            self.auth_state = self.kite.generate_session(request_token, self.user_credentials["API_SECRET"])
            self.kite.set_access_token(self.auth_state["access_token"])
            self.auth_state["state"] = "Logged in"
            self.auth_state["login_time"] = self.auth_state["login_time"].strftime("%Y-%m-%d %H:%M:%S")
            with open(self.access_token_filepath, 'w', encoding='utf-8') as fid:
                json.dump(self.auth_state, fid)
        except Exception:
            self.auth_state["state"] = "Login Error"
            self.auth_state["trace_back"] = traceback.format_exc()
        self.init()

    def init(self):
        self.load_instrument_token_mapper()

    def load_instrument_token_mapper(self, force_refresh: bool = False):
        today = self.today_timestamp()
        filepath = os.path.join(f"{self.cache_path}", f"instruments-{today}.csv")
        if not os.path.exists(filepath) or force_refresh:
            instruments = pd.DataFrame(self.kite.instruments())
            instruments.to_csv(filepath, index=False)
        
        self.instruments = pd.read_csv(filepath)

    def kite_timestamp_to_datetime(self, d):
        return datetime.datetime.strptime(d, "%Y-%m-%d %H:%M:%S")

    def place_order(self, order: Order) -> str:
        order_id = self.kite.place_order(tradingsymbol=order.scrip,
                                         exchange=order.exchange,
                                         transaction_type=order.transaction_type.value,
                                         quantity=order.quantity,
                                         variety=self.kite.VARIETY_REGULAR,
                                         order_type=order.order_type.value,
                                         product=order.product.value,
                                         validity=order.validity)
        return order_id

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

    def download_historic_data(self,
                               scrip:str,
                               exchange: str,
                               interval: str,
                               from_date: Union[datetime.datetime, str],
                               to_date: Union[datetime.datetime, str]) -> bool:
        if isinstance(from_date, str):
            from_date = self.__date_file_date_to_datetime(from_date)
        if isinstance(to_date, str):
            to_date = self.__date_file_date_to_datetime(to_date)

        instrument = self.get_instrument_object({"scrip": scrip, "exchange": exchange})
        subtracting_func = {"days": KiteManager.BATCH_SIZE}

        batch_to_date = to_date
        batch_from_date = batch_to_date - datetime.timedelta(**subtracting_func)
        batch_from_date = max(from_date, batch_from_date)
        exit_condition = False
        self.logger.info(f"Instrument token {instrument['instrument_token']}")
        while ((batch_from_date >= from_date or
                (batch_to_date <= to_date and
                 batch_to_date >= from_date)) and
                 not exit_condition):
            filepath = self.get_filepath_for_data(scrip, exchange, batch_from_date, batch_to_date)
            req_start_time = time.time()
            self.logger.info(f"Started {filepath}")
            if not os.path.exists(filepath):
                data = self.kite.historical_data(instrument["instrument_token"],
                                                interval=interval,
                                                from_date=batch_from_date.strftime("%Y-%m-%d"),
                                                to_date=batch_to_date.strftime("%Y-%m-%d"),
                                                oi=True)
                if len(data) == 0:
                    exit_condition = True
                    continue
                data = pd.DataFrame(data)
                try:
                    data["date"] = data["date"].dt.tz_localize(None)
                except Exception:
                    exit_condition = True
                data.index = data["date"]
                data.drop(["date"], axis=1, inplace=True)
                os.makedirs(os.path.dirname(filepath), exist_ok=True)
                data.to_csv(filepath, index=True)
            else:
                self.logger.info(f"Skipped fetching {filepath} as it already exists...")
                try:
                    data = self.read_data_file(filepath, on_bad_lines='error')
                except Exception:
                    self.logger.info("CSV {filepath} is corrupt.")
                    os.remove(filepath)
                    continue
                if len(data) == 0:
                    break
            batch_to_date = batch_from_date - datetime.timedelta(days=1)
            batch_from_date = batch_from_date - datetime.timedelta(**subtracting_func)
            time_elapsed = time.time() - req_start_time
            self.logger.info(f"Fetching data to {filepath} took {time_elapsed} seconds")
            if time_elapsed < KiteManager.RATE_LIMIT_TIME:
                time.sleep(KiteManager.RATE_LIMIT_TIME - time_elapsed)

    def __date_file_date_to_datetime(self, d):
        return datetime.datetime.strptime(d, "%Y%m%d")

    def get_orders(self):
        orders = self.kite.orders()
        result = []
        for order in orders:
            result.append(Order(order_id=order["order_id"],
                                scrip_id=order["instrument_token"],
                                exchange_id=order["exchange"],
                                scrip=order["exchange_symbol"],
                                exchange=order["exchange"],
                                transaction_type=TransactionType[order["transaction_type"]],
                                raw_dict=order,
                                timestamp=self.kite_timestamp_to_datetime(order["order_timestamp"]),
                                order_type = OrderType[order["order_type"]],
                                product = TradingProduct[order["product"]],
                                quantity = order["quantity"],
                                purchase_price = order["price"],
                                trigger_price = order["trigger_price"],
                                limit_price = order["limit_price"],
                                filled_quantity = order["filled_quantity"],
                                pending_quantity = order["pending_quantity"],
                                cancelled_quantity = order["cancelled_quantity"]))
        return result

    def start_realtime_ticks_impl(self, instruments, *args, **kwargs):
        instruments = self.get_instrument_object(instruments)
        if isinstance(instruments, dict):
            instruments = [instruments]
        self.ticker_instruments = instruments
        self.kws = KiteTicker(self.user_credentials["API_KEY"], self.auth_state["access_token"])
        self.kws.on_ticks = self.__on_ticks
        self.kws.on_connect = self.on_connect_realtime_ticks
        self.kws.on_close = self.on_close_realtime_ticks
        self.logger.info("Starting ticker....")
        #self.kws.connect()
        self.ticker_thread = Thread(target=self.kws.connect, kwargs={"threaded": True})
        self.ticker_thread.start()

    def __on_ticks(self, ws, ticks, *args, **kwargs):
        if self.kill_tick_thread:
            self.kill_tick_thread = False
            raise KeyError("Killed Tick Thread!")
        for tick in ticks:
            self.logger.debug(f"Received tick {tick}")
            ltp = tick["last_price"]
            ltq = tick.get("last_quantity", 0)
            ltt = tick.get("last_trade_time", datetime.datetime.now(pytz.timezone('Asia/Kolkata')))
            key = ltt.strftime("%Y%m%d %H:%M")
            token = tick["instrument_token"]
            self.on_tick(token, ltp, ltq, ltt, key, *args, **kwargs)

    def on_connect_realtime_ticks(self, ws, response, *args, **kwargs):
        self.logger.info(f"Ticker Websock connected {response}.")
        self.logger.info(f"Subscribing to {self.ticker_instruments}")
        tokens = [instrument["instrument_token"] for instrument in self.ticker_instruments]
        self.kws.subscribe(tokens)
        self.kws.set_mode(KiteTicker.MODE_QUOTE, tokens)

    def on_close_realtime_ticks(self, ws, code, reason, *args, **kwargs):
        self.logger.info(f"Ticker Websock closed {code} / {reason}.")

    def get_tick_data(self, instruments=None):
        with self.tick_data_lock:
            if instruments is None:
                return copy.deepcopy(self.tick_data)
            instruments = self.get_instrument_object(instruments)
            result = {}
            for instrument in instruments:
                exchange = instrument["exchange"]
                tradingsymbol = instrument["tradingsymbol"]
                token = instrument["instrument_token"]
                if not exchange in result:
                    result[exchange] = {}
                result[exchange][tradingsymbol] = copy.deepcopy(self.tick_data[token])
            return result
