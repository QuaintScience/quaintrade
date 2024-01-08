import datetime
import copy
import json
import pytz
import os
from threading import Thread
from functools import cache

from typing import Union

from kiteconnect import KiteConnect, KiteTicker
import pandas as pd
import datetime
import time
import traceback

from ..core.ds import Order, OrderType, TradingProduct, TransactionType, Position, OHLCStorageType
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
                    print(self.kite.login_url())
                    response = self.listen_to_login_callback()
                    self.finish_login(response["request_token"])
            return None
        print(self.kite.login_url())
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


class KiteBroker(KiteBaseMixin, Broker):

    def __init__(self,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

    # Order management

    def cancel_pending_orders(self):
        pass

    def get_orders_as_table(self):
        pass
    
    def get_positions_as_table(self):
        pass

    def cancel_order(self, order: Order) -> Order:
        raise NotImplemented(f"Cancel order ({order})")

    def update_order(self, order: Order) -> Order:
        raise NotImplementedError(f"Update order {order}")

    def place_order(self, order: Order) -> Order:
        if not self.dry_mode:
            order_id = self.kite.place_order(tradingsymbol=order.scrip,
                                            exchange=order.exchange,
                                            transaction_type=order.transaction_type.value,
                                            quantity=order.quantity,
                                            variety=self.kite.VARIETY_REGULAR,
                                            order_type=order.order_type.value,
                                            product=order.product.value,
                                            validity=order.validity)
            order.order_id = order_id
        return order

    def order_callback(self, orders):
        raise NotImplementedError("Order Callback")

    def get_positions(self) -> list[Position]:
        raise NotImplementedError("get_positions")

    def place_another_order_on_entry(self,
                                     entry_order: Order,
                                     other_order: Order):
        raise NotImplementedError("place_another_order_on_entry")

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
                                timestamp=self.__kite_timestamp_to_datetime(order["order_timestamp"]),
                                order_type = OrderType[order["order_type"]],
                                product = TradingProduct[order["product"]],
                                quantity = order["quantity"],
                                trigger_price = order["trigger_price"],
                                limit_price = order["limit_price"],
                                filled_quantity = order["filled_quantity"],
                                pending_quantity = order["pending_quantity"],
                                cancelled_quantity = order["cancelled_quantity"]))
        return result


class KiteStreamingDataProvider(KiteBaseMixin, StreamingDataProvider):
    
    def __init__(self, *args, **kwargs):
        StreamingDataProvider.__init__(self, *args, **kwargs)
        KiteBaseMixin.__init__(self, *args, **kwargs)

    def start(self, instruments: list[str], *args, **kwargs):
        instruments = self.get_instrument_object(instruments)
        if isinstance(instruments, dict):
            instruments = [instruments]
        self.ticker_instruments = instruments

        self.kws = KiteTicker(self.auth_credentials["API_KEY"], self.auth_state["access_token"])

        self.kws.on_ticks = self.on_message
        self.kws.on_connect = self.on_connect
        self.kws.on_close = self.on_close
        self.logger.info("Starting ticker....")
        self.ticker_thread = Thread(target=self.kws.connect, kwargs={"threaded": True})
        self.ticker_thread.start()

    def on_connect(self, ws, response, *args, **kwargs):
        self.logger.info(f"Ticker Websock connected {response}.")
        self.logger.info(f"Subscribing to {self.ticker_instruments}")
        tokens = [instrument["instrument_token"] for instrument in self.ticker_instruments]
        self.kws.subscribe(tokens)
        self.kws.set_mode(KiteTicker.MODE_QUOTE, tokens)

    def on_close(self, ws, code, reason, *args, **kwargs):
        self.logger.info(f"Ticker Websock closed {code} / {reason}.")


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
