import datetime
import json
import pickle
import os
import copy
from threading import Thread
import re
from typing import Union

import neo_api_client
import neo_api_client.api_client
import pandas as pd
import datetime
import time
import traceback

from ..core.ds import Order, OrderType, TradingProduct, TransactionType, Position, OHLCStorageType, OrderState
from ..core.roles import HistoricDataProvider, AuthenticatorMixin, Broker, StreamingDataProvider
from ..core.util import today_timestamp, hash_dict, datestring_to_datetime, get_key_from_scrip_and_exchange


class NeoBaseMixin(AuthenticatorMixin):

    ProviderName = "neo"

    def __init__(self, *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

    @staticmethod
    def denormalize_instrument(instrument: dict):
        exchange = instrument["exchange"]
        scrip = instrument["scrip"]
        components = {}
        if exchange == "NSE" or exchange == "BSE":
            if not scrip.endswith("-EQ") and not scrip.lower().startswith("nifty") and not scrip.lower().startswith("banknifty"):
                scrip = f'{scrip}-EQ'
        if instrument["exchange"] == "NFO":
            exchange = "nse_fo"
            components = re.match(r"(?P<scrip>[A-Z]+)(?P<expiry>[0-9]{2}[A-Z]{3})(?P<strike>[0-9]+)(?P<typ>(PE|CE))", scrip).groupdict()
        elif instrument["exchange"] == "NSE":
            exchange = "nse_cm"
        return {"scrip": scrip, "exchange": exchange, **components}

    def login(self):

        self.auth_state = {"state": "Start Login"}
        if self.reset_auth_cache:
            if os.path.exists(self.access_token_filepath):
                os.remove(self.access_token_filepath)

        if os.path.exists(self.access_token_filepath):
            self.logger.info(f"Loading access token from {self.access_token_filepath}")
            with open(self.access_token_filepath, 'rb') as fid:
                try:
                    configuration = pickle.load(fid)
                    self.logger.info(f"Found auth state {self.auth_state}")
                    consumer_key = self.auth_credentials["CONSUMER_KEY"]
                    # app_secret key which you got after creating the app 
                    consumer_secret = self.auth_credentials["CONSUMER_SECRET"]
                    # The grant_type always has to be "authorization_code"
                    client = neo_api_client.NeoAPI(consumer_key=consumer_key, consumer_secret=consumer_secret, environment='prod',
                                                 access_token=None, neo_fin_key=None)
                    client.configuration = configuration
                    client.api_client = neo_api_client.api_client.ApiClient(configuration)
                    self.client = client
                except Exception:
                    self.logger.info(f"Loading state failed.")
                    self.start_login()
        else:
            for item in self.start_login():
                yield item
        for item in self.finish_login():
            yield item

    def start_login(self):
        self.auth_inputs = {}
        # redircet_uri you entered while creating APP.
        redirect_uri = self.auth_credentials["REDIRECT_URI"] 
        # Client_id here refers to APP_ID of the created app
        consumer_key = self.auth_credentials["CONSUMER_KEY"]
        # app_secret key which you got after creating the app 
        consumer_secret = self.auth_credentials["CONSUMER_SECRET"]
        print(self.auth_credentials)
        # The grant_type always has to be "authorization_code"
        client = neo_api_client.NeoAPI(consumer_key=consumer_key, consumer_secret=consumer_secret, environment='prod',
                                       access_token=None, neo_fin_key=None)

        yield {"text": "mobile", "field": "mobile"}
        yield {"text": "password", "field": "password"}
        print(client.login(mobilenumber=f"+91{self.auth_inputs['mobile']}", password=self.auth_inputs["password"]))
        yield {"text": "OTP", "field": "otp"}
        client.session_2fa(OTP=self.auth_inputs["otp"])
        self.client = client
        with open(self.access_token_filepath, 'wb') as fid:
            pickle.dump(client.configuration, fid)

    def enrich_with_instrument_code(self, instrument):
        instrument = copy.deepcopy(instrument)
        if "expiry" in instrument:
                expiry_plus_one = (datetime.datetime.strptime(instrument["expiry"], "%d%m%Y") + datetime.timedelta(days=1)).strftime("%d%m%Y")
        else:
            expiry_plus_one = ""
        res = self.client.search_scrip(exchange_segment=instrument["exchange"],
                                        symbol=instrument["scrip"],
                                        expiry=expiry_plus_one,
                                        option_type=instrument.get("type", ""),
                                        strike_price=instrument.get("strike", ""))
        if len(res) == 0:
            self.logger.warn(f"Could not find instrument code for {instrument}")
        instrument["instrument_code"] = res[0]["pSymbol"]
        instrument["scrip"] = instrument["pTrdSymbol"]
        return instrument


    def finish_login(self):
       return []

    def init(self):
        pass

class NeoStreamingDataProvider(NeoBaseMixin, StreamingDataProvider):
    
    def __init__(self, *args, **kwargs):
        StreamingDataProvider.__init__(self, *args, **kwargs)
        NeoBaseMixin.__init__(self, *args, **kwargs)

    def start(self, instruments: list[str], *args, **kwargs):

        for instrument in instruments:
            instrument = self.enrich_with_instrument_code(instrument)

        self.ticker_instruments = instruments
        self.logger.info("Starting ticker....")
        self.client.subscribe(instrument_tokens = self.ticker_instruments, isIndex=False, isDepth=False)
        self.client.on_message = self.on_message  # called when message is received from websocket
        self.client.on_error = self.on_error  # called when any error or exception occurs in code or websocket
        self.client.on_close = self.on_close  # called when websocket connection is closed
        self.client.on_open = self.on_connect  # called when websocket successfully connects

    def on_connect(self, ws, response, *args, **kwargs):
        self.logger.info(f"Ticker Websock connected {response}.")
        #self.logger.info(f"Subscribing to {self.ticker_instruments}")
        #self.fws.subscribe(self.ticker_instruments)        client.session_2fa(OTP=self.auth_inputs["otp"])

        #self.fws.keep_running()

    def on_close(self, ws, code, reason, *args, **kwargs):
        self.logger.info(f"Ticker Websock closed {code} / {reason}.")

    def on_message(self, message, *args, **kwargs):
        if self.kill_tick_thread:
            self.kill_tick_thread = False
            raise KeyError("Killed Tick Thread!")
        self.logger.debug(message)
        if message["type"] == "sf":
            try:
                ltp = message["ltp"]
                exchange, scrip = message["ts"], message["e"]
                token = f"{scrip}:{exchange}"
                ltq = message["ltq"]
                ltt = datetime.datetime.fromtimestamp(message["ltt"])
                self.on_tick(token=token,
                             ltp=ltp,
                             ltq=ltq,
                             ltt=ltt)
            except:
                traceback.print_exc()


class NeoBrokerProvider(NeoBaseMixin,
                        Broker):

    ProviderName = "neo"

    def __init__(self,
                 *args,
                 **kwargs):
        self.orders_cache = []
        self.positions_cache = []
        self.rate_limit_time: float = 0.33
        Broker.__init__(self, *args, **kwargs)
        NeoBaseMixin.__init__(self, *args, **kwargs)

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
                        self.client.cancel_order(order_id=order.order_id, isVerify=False)
                        time.sleep(self.rate_limit_time)
                    except Exception:
                        self.logger.warn(f"Could not delete order {order.order_id}")
                        storage = self.get_tradebook_storage()
                        storage.store_order_execution(self.strategy, self.run_name,
                                                        run_id=self.run_id,
                                                        date=self.current_datetime(),
                                                        order=order, event="OrderCancelledFailed")
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
        instrument = self.enrich_with_instrument_code(NeoBaseMixin.denormalize_instrument({"scrip": order.scrip, "exchange": order.exchange}))
        for existing_order in self.get_orders(refresh_cache=refresh_cache):
            if existing_order.order_id == order.order_id:
                if local_update:
                    existing_order.tags = order.tags
                    existing_order.parent_order_id = order.parent_order_id
                    existing_order.group_id = order.group_id
                
                if existing_order.state == OrderState.PENDING and not local_update:
                    self.logger.info(f"Found order with order_id {order.order_id} for updation...")
                    order_id = self.client.modify_order(order_id=order.order_id,
                                                        quantity=int(order.quantity),
                                                        trigger_price=round(order.trigger_price, 1),
                                                        order_type=self.__translate_order_type(order.order_type),
                                                        transaction_type=self.__translate_transaction_type(order.transaction_type),
                                                        trading_symbol=instrument["scrip"],
                                                        product=self.__translate_product(order.product))
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
            return "SL"
        elif order.order_type == OrderType.MARKET:
            return "MKT"
        elif order.order_type == OrderType.LIMIT:
            return "L"
        elif order.order_type == OrderType.SL_MARKET:
            return "SL-M"

    def __translate_transaction_type(self, order: Order):
        if order.transaction_type == TransactionType.BUY:
            return "B"

        elif order.transaction_type == TransactionType.SELL:
            return "S"
    
    def __translate_product(self, order: Order):
        if order.product == TradingProduct.MIS:
            return "MIS"
        elif order.price == TradingProduct.CNC:    
            return "CNC"
        elif order.price == TradingProduct.NRML:
            return "NRML"

    def place_order(self, order: Order, refresh_cache: bool = True) -> Order:
        instrument = self.enrich_with_instrument_code(NeoBaseMixin.denormalize_instrument({"scrip": order.scrip, "exchange": order.exchange}))
        order_kwargs = {"trading_symbol": instrument["scrip"],
                        "exchange_segment": instrument["exchange"],
                        "transaction_type": self.__translate_transaction_type(order),
                        "quantity": int(order.quantity),
                        "order_type": self.__translate_order_type(order),
                        "product": self.__translate_product(order),
                        "validity": order.validity,
                        "pf": "N",
                        "market_protection": "0",
                        "disclosed_quantity": "0",
                        "AMO": "NO",
                        "tag": "QuaintScalp"}
        if order.order_type in [OrderType.LIMIT, OrderType.SL_LIMIT]:
            order_kwargs["price"] = round(order.limit_price, 1)
        if order.order_type in [OrderType.SL_LIMIT, OrderType.SL_MARKET]:
            order_kwargs["trigger_price"] = round(order.trigger_price, 1)
        self.logger.info(f"NEO: {order_kwargs}")
        resp = {}
        try:
            resp = self.client.place_order(**order_kwargs)
            order.order_id = resp["nOrdNo"]
        except Exception:
            traceback.print_exc()
            return None
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
        if order_type == "L":
            return OrderType.LIMIT
        elif order_type == "MKT":
            return OrderType.MARKET
        elif order_type == "SL":
            return OrderType.SL_LIMIT
        elif order_type == "SL-M":
            return OrderType.SL_MARKET
        else:
            raise ValueError(f"Unknown order type {order_type}")
    
    def __reverse_translate_product(self, product: str):
        if product == "CNC":
            return TradingProduct.CNC
        elif product == "MIS":
            return TradingProduct.MIS
        elif product == "NRML":
            return TradingProduct.NRML
        else:
            raise ValueError(f"Unknown product {product}")

    def __reverse_translate_transaction_type(self, transaction_type: str):
        if transaction_type == "B":
            return TransactionType.BUY
        elif transaction_type == "S":
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

    def start_streamer(self): # FIXME
        pass

    def start_order_change_streamer(self):
        self.start_streamer()
