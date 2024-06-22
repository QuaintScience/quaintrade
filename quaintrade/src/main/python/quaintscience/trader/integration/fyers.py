import datetime
import json
import os
from threading import Thread

from typing import Union

from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws
import pandas as pd
import datetime
import time
import traceback

from ..core.ds import Order, OrderType, TradingProduct, TransactionType, Position, OHLCStorageType
from ..core.roles import HistoricDataProvider, AuthenticatorMixin, Broker, StreamingDataProvider
from ..core.util import datestring_to_datetime


class FyersBaseMixin(AuthenticatorMixin):

    ProviderName = "fyers"

    def __init__(self, *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

    def login(self):

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
                except Exception:
                    for output in self.generate_access_token():
                        yield output
        else:
            for output in self.generate_access_token():
                yield output
        yield self.finish_login()

    @staticmethod
    def denormalize_instrument(instrument: dict):
        scrip = instrument["scrip"]
        exchange = instrument["exchange"]
        if ">" in scrip:
            ty, index, loc, strike, expiry = [v.strip() for v in scrip.split(">")]
            expiry = datetime.datetime.strptime(expiry, "%Y%m%d")
            scrip = f"{index}{expiry.strftime('%y%b').upper()}{strike}{ty}"
        else:
            if exchange == "NSE" or exchange == "BSE":
                if not scrip.endswith("-EQ") and not scrip.lower().startswith("nifty") and not scrip.lower().startswith("banknifty"):
                    scrip = f'{scrip}-EQ'
        if exchange == "NFO":
            exchange = "NSE"
        return {"scrip": scrip, "exchange": exchange}

    def generate_access_token(self):
        # redircet_uri you entered while creating APP.
        redirect_uri = self.auth_credentials["REDIRECT_URI"] 
        # Client_id here refers to APP_ID of the created app
        client_id = self.auth_credentials["CLIENT_ID"]
        # app_secret key which you got after creating the app 
        secret_key = self.auth_credentials["SECRET_KEY"]
        # The grant_type always has to be "authorization_code"
        grant_type = "authorization_code"
        # The response_type always has to be "code"
        response_type = "code"
        #  The state field here acts as a session manager. 
        # you will be sent with the state field after successfull generation of auth_code 
        state = "sample"
        appSession = fyersModel.SessionModel(client_id=client_id,
                                             redirect_uri=redirect_uri,
                                             response_type=response_type,
                                             state=state,
                                             secret_key=secret_key,
                                             grant_type=grant_type)
        generateTokenUrl = appSession.generate_authcode()
        print(generateTokenUrl, flush=True)
        yield generateTokenUrl
        
        response = self.listen_to_login_callback()
        appSession.set_token(response["query_params"]["auth_code"][0])
        response = appSession.generate_token()
        self.auth_state["access_token"] = response["access_token"]
        self.auth_state["state"] = "Logged in"
        self.auth_state["login_time"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(self.access_token_filepath, 'w', encoding='utf-8') as fid:
            json.dump(self.auth_state, fid)
        yield True

    def finish_login(self):
        try:
            self.fyers = fyersModel.FyersModel(token=self.auth_state["access_token"],
                                                is_async=False,
                                                client_id=self.auth_credentials["CLIENT_ID"],
                                                log_path="")
            return True
        except Exception:
            self.auth_state["state"] = "Login Error"
            self.auth_state["traceback"] = traceback.format_exc()
            return False

    def init(self):
        pass


class FyersHistoricDataProvider(FyersBaseMixin, HistoricDataProvider):

    ProviderName = "fyers"

    def __init__(self,
                 *args,
                 rate_limit_time: float = 0.33,
                 batch_size: int = 59,
                 **kwargs):
        HistoricDataProvider.__init__(self, *args, **kwargs)
        FyersBaseMixin.__init__(self, *args, **kwargs)
        self.rate_limit_time = rate_limit_time
        self.batch_size = batch_size

    def download_historic_data(self,
                               scrip:str,
                               exchange: str,
                               interval: str,
                               from_date: Union[datetime.datetime, str],
                               to_date: Union[datetime.datetime, str],
                               finegrained: bool = False) -> bool:
        if interval == "1min":
            interval = "1"

        normalized_instrument = FyersBaseMixin.denormalize_instrument({"scrip": scrip, "exchange": exchange})
        scrip = normalized_instrument["scrip"]
        exchange = normalized_instrument["exchange"]

        if isinstance(from_date, str):
            from_date = datestring_to_datetime(from_date)
        if isinstance(to_date, str):
            to_date = datestring_to_datetime(to_date)
        
        self.logger.info(f"Start fetch data from fyers {from_date} to {to_date}")
        if not finegrained:
            from_date = from_date.strftime("%Y-%m-%d")
            to_date = to_date.strftime("%Y-%m-%d")
        else:
            self.logger.info(f"Converting datetime into timestamp for finegrained retrieval")
            from_date = from_date.strftime("%s")
            to_date = to_date.strftime("%s")

        req_start_time = time.time()
        req = {"symbol": f"{exchange}:{scrip}",
               "resolution": interval,
               "date_format": "1" if not finegrained else "0",
               "range_from": from_date,
               "range_to": to_date,
               "cont_flag": "1"}
        self.logger.debug(f"Req: {req}")
        data = self.fyers.history(req)
        if len(data) == 0 or data["s"] == "error":
            print(f"No data or error: {data}")
            return False
        data = pd.DataFrame(data["candles"], columns=["date", "open", "high", "low", "close", "volume"])
        data["date"] = pd.to_datetime(data["date"], unit='s').dt.tz_localize('utc').dt.tz_convert('Asia/Kolkata').dt.tz_localize(None)
        data.index = data["date"]
        data.drop(["date"], axis=1, inplace=True)
        data["oi"] = 0

        storage = self.get_storage(scrip, exchange, storage_type=OHLCStorageType.PERM)
        storage.put(scrip, exchange, data)
        time_elapsed = time.time() - req_start_time
        self.logger.info(f"Fetching {len(data)} rows of data from fyers {from_date} to {to_date} took {time_elapsed:.2f} seconds")
        if time_elapsed < self.rate_limit_time:
            time.sleep(self.rate_limit_time - time_elapsed)
        return True


class FyersStreamingDataProvider(FyersBaseMixin, StreamingDataProvider):
    
    def __init__(self, *args, **kwargs):
        StreamingDataProvider.__init__(self, *args, **kwargs)
        FyersBaseMixin.__init__(self, *args, **kwargs)
    
    def unsubscribe(self, instruments: list[str]):
        instruments = [self.denormalize_instrument(instrument) for instrument in instruments]
        ticker_instruments = [f"{instrument['exchange']}:{instrument['scrip']}" for instrument in instruments]
        self.fws.unsubscribe(ticker_instruments)
        self.ticker_instruments = list(set(self.ticker_instruments).difference(set(ticker_instruments)))
    
    def subscribe(self, instruments: list[str]):
        instruments = [self.denormalize_instrument(instrument) for instrument in instruments]
        ticker_instruments = [f"{instrument['exchange']}:{instrument['scrip']}" for instrument in instruments]
        self.fws.subscribe(ticker_instruments)
        self.ticker_instruments = list(set(self.ticker_instruments).union(set(ticker_instruments)))

    def start(self, instruments: list[str], *args, **kwargs):
        instruments = [self.denormalize_instrument(instrument) for instrument in instruments]
        self.ticker_instruments = [f"{instrument['exchange']}:{instrument['scrip']}" for instrument in instruments]

        self.fws = data_ws.FyersDataSocket(access_token=self.auth_state["access_token"],
                                           log_path="",
                                           litemode=False,
                                           write_to_file=False,
                                           reconnect=True,
                                           on_connect=self.on_connect,
                                           on_close=self.on_close,
                                           on_error=self.on_error,
                                           on_message=self.on_message)

        self.logger.info("Starting ticker....")
        self.fws.connect()
        self.fws.subscribe(self.ticker_instruments)
        self.fws.keep_running()

    def on_connect(self, ws, response, *args, **kwargs):
        self.logger.info(f"Ticker Websock connected {response}.")
        self.logger.info(f"Subscribing to {self.ticker_instruments}")
        self.fws.subscribe(self.ticker_instruments)
        self.fws.keep_running()

    def on_close(self, ws, code, reason, *args, **kwargs):
        self.logger.info(f"Ticker Websock closed {code} / {reason}.")

    def on_message(self, message, *args, **kwargs):
        print(message)
        if self.kill_tick_thread:
            self.kill_tick_thread = False
            raise KeyError("Killed Tick Thread!")
        self.logger.debug(message)
        if message["type"] == "sf":
            try:
                ltp = message["ltp"]
                exchange, scrip = message["symbol"].split(":")
                token = f"{scrip}:{exchange}"
                ltq = message["last_traded_qty"]
                ltt = datetime.datetime.fromtimestamp(message["last_traded_time"])
                self.on_tick(token=token,
                            ltp=ltp,
                            ltq=ltq,
                            ltt=ltt)
            except:
                traceback.print_exc()
