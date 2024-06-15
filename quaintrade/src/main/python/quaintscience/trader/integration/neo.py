import datetime
from typing import Optional
import json
import pickle
import os
import traceback
import copy
from threading import Thread
import re
from typing import Union

import neo_api_client
from neo_api_client.api_client import ApiClient
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
        self.auth_inputs = {}
        super().__init__(*args, **kwargs)

    @staticmethod
    def denormalize_instrument(instrument: dict):
        exchange = instrument["exchange"]

        scrip = instrument["scrip"]
        if "expiry" in instrument: # Already denormalized.
            return
        if ">" in scrip:
            ty, index, loc, strike, expiry = [v.strip() for v in scrip.split(">")]
            expiry = datetime.datetime.strptime(expiry, "%Y%m%d")
            scrip = f"{index}{expiry.strftime('%y%-m%d')}{strike}{ty}"
            return {"scrip": scrip, "exchange": exchange, "expiry": expiry, "strike": strike, "typ": ty}
        components = {}
        if exchange == "NSE" or exchange == "BSE":
            if not scrip.endswith("-EQ") and not scrip.lower().startswith("nifty") and not scrip.lower().startswith("banknifty"):
                scrip = f'{scrip}-EQ'
        if instrument["exchange"] == "NFO":
            exchange = "nse_fo"
            print(scrip)
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
                    client = neo_api_client.NeoAPI(access_token=None, environment='prod',
                                                   consumer_key=consumer_key, consumer_secret=consumer_secret,
                                                   configuration=configuration)
                    self.client = client
                except Exception:
                    traceback.print_exc()
                    self.logger.info(f"Loading state failed.")
                    for item in self.start_login():
                        yield item
                    for item in self.finish_login():
                        yield item
        else:
            for item in self.start_login():
                yield item
            for item in self.finish_login():
                yield item

    def start_login(self):
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
        self.client = client
        yield {"text": "mobile", "field": "mobile"}
        yield {"text": "password", "field": "password"}
        print(self.client.login(mobilenumber=f"+91{self.auth_inputs['mobile']}", password=self.auth_inputs["password"]))
        yield {"text": "OTP", "field": "otp"}

    def enrich_with_instrument_code(self, instrument):
        instrument = self.denormalize_instrument(instrument)
        instrument = copy.deepcopy(instrument)
        if "expiry" in instrument:
                expiry = instrument["expiry"]
                if isinstance(expiry, str):
                    expiry = datetime.datetime.strptime(expiry, "%d%m%Y")
                expiry_plus_one = (expiry + datetime.timedelta(days=1)).strftime("%d%b%Y")
        else:
            expiry_plus_one = ""
        self.logger.debug(f"Searching for scrip {instrument} {expiry_plus_one}")
        scrip = instrument["scrip"]
        if expiry_plus_one != "":
            if scrip.startswith("NIFTY"):
                scrip = "NIFTY"
            elif scrip.startswith("BANKNIFTY"):
                scrip = "BANKNIFTY"
        res = self.client.search_scrip(exchange_segment=instrument["exchange"],
                                       symbol=scrip,
                                       expiry=expiry_plus_one,
                                       option_type=instrument.get("type", ""),
                                       strike_price=instrument.get("strike", ""))
        if len(res) == 0 or not isinstance(res, list):
            self.logger.warn(f"Could not find instrument code for {instrument}")
        print(res)
        instrument["instrument_code"] = res[0]["pSymbol"]
        instrument["scrip"] = res[0]["pTrdSymbol"]
        return instrument


    def finish_login(self):
        self.client.session_2fa(OTP=self.auth_inputs["otp"])
        with open(self.access_token_filepath, 'wb') as fid:
            pickle.dump(self.client.configuration, fid)
        return iter(())


    def init(self):
        pass

class NeoStreamingMixin:

    def __init__(self, *args,
                 on_message: Optional[callable] = None,
                 on_connect: Optional[callable] = None,
                 on_close: Optional[callable] = None,
                 on_order_update: Optional[callable] = None,
                 on_error: Optional[callable] = None,
                 **kwargs):
        if on_message is None and on_order_update is None:
            raise ValueError("Streaming cannot work when both on_message and on_orders is None.")
        if on_close is None:
            on_close = self.on_close
        if on_connect is None:
            on_connect = self.on_connect
        if on_error is None:
            on_error = self.on_error
        self.on_message_callable = on_message
        self.on_connect_callable = on_connect
        self.on_close_callable = on_close
        self.on_order_update_callable = on_order_update
        self.on_error_callable = on_error
        
    def on_connect(self, message):
        self.logger.info(f"Neo Websock connected {message}.")
        #self.logger.info(f"Subscribing to {self.ticker_instruments}")
        #self.fws.subscribe(self.ticker_instruments)        client.session_2fa(OTP=self.auth_inputs["otp"])

        #self.fws.keep_running()

    def on_close(self, message):
        self.logger.info(f"Neo Websock closed {message}.")
    
    def on_error(self, message):
        self.logger.error(f"Neo Websock error {message}.")

    def start(self, instruments: list[str] = None, *args, **kwargs):
        if instruments is None:
            instruments = []
        for instrument in instruments:
            instrument = self.enrich_with_instrument_code(instrument)

        self.ticker_instruments = instruments
        self.logger.info("Starting ticker....")
        if self.on_message_callable is not None:
            self.client.on_message = self.on_message_callable  # called when message is received from websocket
        if self.on_order_update_callable is not None:
            self.client.on_message = self.on_order_update_callable  # called when message is received from websocket
        if self.on_error_callable is not None:
            self.client.on_error = self.on_error_callable  # called when any error or exception occurs in code or websocket
        if self.on_close_callable is not None:
            self.client.on_close = self.on_close_callable  # called when websocket connection is closed
        if self.on_connect_callable is not None:
            self.client.on_open = self.on_connect_callable  # called when websocket successfully connects
        if self.on_message_callable is not None:
            self.client.subscribe(instrument_tokens = self.ticker_instruments, isIndex=False, isDepth=False)
        else:
            self.client.subscribe_to_orderfeed()
    
class NeoStreamingDataProvider(NeoBaseMixin, NeoStreamingMixin, StreamingDataProvider):
    
    def __init__(self, *args, **kwargs):
        NeoBaseMixin.__init__(self, *args, **kwargs)
        kwargs["on_message"] = self.on_message
        NeoStreamingMixin.__init__(self, *args, **kwargs)
        StreamingDataProvider.__init__(self, *args, **kwargs)

    def on_message(self, message):
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
                        NeoStreamingMixin,
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
        kwargs["on_order_update"] = self.order_callback
        kwargs["on_error"] = self.error_callback
        NeoStreamingMixin.__init__(self, *args, **kwargs)

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
        instrument = self.enrich_with_instrument_code({"scrip": order.scrip, "exchange": order.exchange})
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
        instrument = self.enrich_with_instrument_code({"scrip": order.scrip, "exchange": order.exchange})
        order_kwargs = {"trading_symbol": instrument["scrip"],
                        "exchange_segment": instrument["exchange"],
                        "transaction_type": self.__translate_transaction_type(order),
                        "quantity": str(int(order.quantity)),
                        "order_type": self.__translate_order_type(order),
                        "product": self.__translate_product(order),
                        "validity": order.validity,
                        "pf": "N",
                        "market_protection": "0",
                        "disclosed_quantity": "0",
                        "amo": "NO",
                        "tag": "QuaintScalp"}
        if order.order_type in [OrderType.LIMIT, OrderType.SL_LIMIT]:
            order_kwargs["price"] = str(round(order.limit_price, 1))
        if order.order_type in [OrderType.SL_LIMIT, OrderType.SL_MARKET]:
            order_kwargs["trigger_price"] = str(round(order.trigger_price, 1))
        self.logger.info(f"NEO: {order_kwargs}")
        resp = {}
        try:
            resp = self.client.place_order(**order_kwargs)
            print(resp)
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

    def order_callback(self, message):
        self.logger.info(f"Received order update {message}")
        # self.get_orders(refresh_cache=True) # Commented as gtt_order_callback does this anyway.
        # self.__update_order_in_cache(message) # Locks order cache
        # self.__update_gtt_orders_using_dct(message) # Locks gtt
        self.gtt_order_callback(refresh_cache=True) # Locks order cache intermittently and locks gtt

    def error_callback(self, *args, **kwargs):
        self.gtt_order_callback(refresh_cache=True) # Locks order cache intermittently and locks gtt

    def __get_avg_price(self, position: dict):
        floats = ["cfBuyAmt", "buyAmt", "cfSellAmt", "sellAmt", "flBuyQty", "cfBuyQty", "flSellQty", "cfSellQty", "genNum", "genDen", "prcNum", "prcDen", "multiplier", "precision"]
        for f in floats:
            position[f] = float(position[f])
        buy_amount = (position["cfBuyAmt"] + position["buyAmt"])
        sell_amount = (position["cfSellAmt"] + position["sellAmt"])
        buy_avg_price = buy_amount / ((position["cfBuyQty"] + position["flBuyQty"]) * position["multiplier"] * (position["genNum"] / position["genDen"]) * (position["prcNum"] / position["prcDen"]))
        sell_avg_price = sell_amount / ((position["cfSellQty"] + position["flSellQty"]) * position["multiplier"] * (position["genNum"] / position["genDen"]) * (position["prcNum"] / position["prcDen"]))
        avg_price = 0
        buy_quantity = position["cfBuyQty"] + position["flBuyQty"]
        sell_quantity = position["cfSellQty"] + position["flSellQty"]
        if buy_quantity > sell_quantity:
            avg_price = buy_avg_price
        elif sell_quantity > buy_quantity:
            avg_price = sell_avg_price
        else:
            avg_price = 0.
        return round(avg_price, int(position["precision"])), buy_quantity, sell_quantity
    

    def get_positions(self, refresh_cache: bool = True) -> list[Position]:
        if refresh_cache:
            resp = self.client.positions()
            positions = []
            if "data" in resp:
                positions = resp["data"]
            else:
                raise IOError("Positions returned error")
        else:
            positions = {"day": [], "net": []}
        for position in positions:
            found_position_in_cache = False
            with self.position_state_lock:
                for existing_position in self.positions_cache:
                    # print(existing_position, position)
                    avg_price, buy_qty, sell_qty = self.__get_avg_price(position)
                    print(position)
                    if (existing_position.scrip == position["trdSym"]
                        and existing_position.exchange == position["exSeg"]
                        and existing_position.product == self.__reverse_translate_product(position["prod"])):
                        found_position_in_cache = True
                        existing_position.quantity = buy_qty - sell_qty
                        existing_position.last_price = 0.
                        existing_position.pnl = 0.
                        existing_position.average_price = avg_price
                        existing_position.timestamp = self.current_datetime()
                        break
                if not found_position_in_cache:
                    avg_price, buy_qty, sell_qty = self.__get_avg_price(position)
                    new_position = Position(scrip_id=position["trdSym"],
                                            scrip=position["trdSym"],
                                            exchange=position["exSeg"],
                                            exchange_id=position["exSeg"],
                                            product=self.__reverse_translate_product(position["prod"]),
                                            last_price=0.,
                                            pnl=0.,
                                            quantity = buy_qty - sell_qty,
                                            timestamp=self.current_datetime(),
                                            average_price=avg_price)
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
        elif product == "MIS" or product == "BO":
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
        if order_state == "cancelled":
            return OrderState.CANCELLED
        elif order_state == "complete":
            return OrderState.COMPLETED
        elif order_state == "rejected":
            return OrderState.REJECTED
        elif order_state == "trigger pending":
            return OrderState.PENDING
        elif order_state == "open":
            return OrderState.PENDING
        elif order_state == "open pending":
            return OrderState.PENDING
        elif order_state == "validation pending":
            return OrderState.PENDING
        else:
            raise ValueError(f"Unknown Order State {order_state}")

    def __update_order_from_dct(self, cached_order: Order, order: dict):
        cached_order.quantity = int(order["qty"])
        cached_order.trigger_price = order["trgPrc"]
        cached_order.limit_price = float(order["prc"])
        cached_order.filled_quantity = int(order["fldQty"])
        cached_order.pending_quantity = int(order["unFldSz"])
        # cached_order.cancelled_quantity = order["cancelled_quantity"]
        cached_order.state = self.__reverse_translate_order_state(order["ordSt"])
        cached_order.raw_dict = order

    def __update_order_in_cache(self,
                                order: dict):

        found_in_cache = False
        with self.order_state_lock:
            for cached_order in self.orders_cache:
                if order["nOrdNo"] == cached_order.order_id:
                    self.logger.debug(f"Updated cached order {order['nOrdNo']}")
                    self.__update_order_from_dct(cached_order=cached_order, 
                                                order=order)
                    found_in_cache = True
                    break
            if not found_in_cache:
                print(order)
                self.logger.debug(f"Creating new order in cache for {order['exOrdId']}")
                print(order)
                new_order = Order(order_id=order["nOrdNo"],
                                  exchange_id=order["exSeg"],
                                  scrip=order["trdSym"],
                                  scrip_id=order["trdSym"],
                                  exchange=order["exSeg"],
                                  transaction_type=self.__reverse_translate_transaction_type(order["trnsTp"]),
                                  raw_dict=order,
                                  state=self.__reverse_translate_order_state(order["ordSt"]),
                                  timestamp=order["ordDtTm"],
                                  order_type = self.__reverse_translate_order_type(order["prcTp"]),
                                  product = self.__reverse_translate_product(order["prod"]),
                                  quantity = int(order["qty"]),
                                  trigger_price = float(order["trgPrc"]),
                                  limit_price = float(order["prc"]),
                                  filled_quantity = int(order["fldQty"]),
                                  pending_quantity = int(order["unFldSz"]),
                                  cancelled_quantity = 0)
                self.orders_cache.append(new_order)

    def get_orders(self, refresh_cache=True) -> list[Order]:

        if refresh_cache:
            resp = self.client.order_report()
            if "data" not in resp:
                print(resp, "Error")
                raise IOError("Response not valid: Neo")
            orders = resp["data"]
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
                if from_order.order_id == order["nOrdNo"]:
                    self.__update_order_from_dct(from_order, order)
