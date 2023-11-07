from abc import ABC, abstractmethod
import datetime
import os
from threading import Lock
import http.server
import socketserver
from urllib.parse import urlparse, parse_qs

import talib
import pandas as pd
import redis

from ..core.logging import LoggerMixin
from ..core.ds import Order


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


class TradeManager(LoggerMixin):

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
        if self.redis_server is not None:
            self.redis = redis.Redis(self.redis_server,
                                     self.redis_port)
        else:
            self.redis = None
        super().__init__(*args, **kwargs)

    def listen_to_login_callback(self):
        return OAuthCallBackServer.get_oauth_callback_data(self.oauth_callback_port)

    def today_timestamp(self):
        return datetime.datetime.now().strftime("%Y%m%d")

    def postprocess_data(self,
                           data,
                           interval,
                           donchain_N=15):
        data.fillna(0., inplace=True)
        if "time" in data.columns and "date" in data.columns:
            data["timestamp"] = pd.to_datetime(data["date"] + ", " + data["time"])
            data.drop(["date", "time"], inplace=True, axis=1)
            data.index = data["timestamp"]
        data.dropna(inplace=True)
        data = data.resample(interval).apply({'open': 'first',
                                              'high': 'max',
                                              'low': 'min',
                                              'close': 'last'})
        data.dropna(inplace=True)
        data["donchainUpper"] = data["high"].rolling(donchain_N).apply(lambda x: max(x))
        data["donchainLower"] = data["low"].rolling(donchain_N).apply(lambda x: min(x))
        data["donchainMiddle"] = (data["donchainLower"] + data["donchainUpper"]) /2

        pwh = data["high"].resample('1w').apply("max").shift(1, freq='d')
        data["pwh"] = pwh.resample(interval).ffill().ffill()
        pwl = data["low"].resample('1w').apply("min").shift(1, freq='d')
        data["pwl"] = pwl.resample(interval).ffill().ffill()

        pdh = data["high"].resample('1d').apply("max").shift(1,
                                                             freq='d')
        data["pdh"] = pdh.resample(interval).ffill().ffill()
        pdl = data["low"].resample('1d').apply("min").shift(1,
                                                            freq='d')
        data["pdl"] = pdl.resample(interval).ffill().ffill()

        pdc = data["close"].resample('1d').apply("last").shift(1, freq='d')
        data["pdc"] = pdh.resample(interval).ffill().ffill()
        pdo = data["open"].resample('1d').apply("first").shift(1, freq='d')
        data["pdo"] = pdo.resample(interval).ffill().ffill()

        cdh = data["high"].resample('1d').apply("max")
        data["cdh"] = cdh.resample(interval).ffill().ffill()
        cdl = data["low"].resample('1d').apply("min")
        data["cdl"] = cdl.resample(interval).ffill().ffill()

        cdc = data["close"].resample('1d').apply("last")
        data["cdc"] = cdc.resample(interval).ffill().ffill()
        cdo = data["open"].resample('1d').apply("first")
        data["cdo"] = cdo.resample(interval).ffill().ffill()
        for c in data.columns:
            data[c] = data[c].astype(float)
        #data.dropna(inplace=True)
        data["breakoutCandidateUpper"] = 0
        data.loc[(data["close"] > data["donchainUpper"].shift()) &
                 (data.index.hour < 15) &
                 ((data.index.hour > 9) |
                  (data.index.minute > 30)),
                  "breakoutCandidateUpper"] = 1.0
        data["breakoutCandidateLower"] = 0
        data.loc[(data["close"] < data["donchainLower"].shift()) &
                 (data.index.hour < 15) &
                 ((data.index.hour > 9) |
                  (data.index.minute > 30)),
                  "breakoutCandidateLower"] = 1.0

        data["breakoutCandidate"] = ((data["breakoutCandidateUpper"] == 1) |
                                     (data["breakoutCandidateLower"] == 1))
        data["sma_22"] = talib.SMA(data["close"], timeperiod=22)
        data["sma_33"] = talib.SMA(data["close"], timeperiod=33)
        data["sma_44"] = talib.SMA(data["close"], timeperiod=44)
        data["adx"] = talib.ADX(data["high"], data["l"], data["c"])
        data["rsi"] = talib.RSI(data["close"])
        data["atr"] = talib.ATR(data["high"], data["l"], data["c"])
        data["bbands_upper"], data["bbands_middle"], data["bbands_lower"] = talib.BBANDS(data["close"])
        for item in dir(talib):
            if item.startswith("CDL"):
                data[item] = getattr(talib, item)(data["o"], data["h"], data["l"], data["c"])
        return data

    @abstractmethod
    def start_login(self) -> str:
        pass

    @abstractmethod
    def finish_login(self, *args, **kwargs) -> bool:
        pass

    @abstractmethod
    def init(self):
        pass

    def get_state(self):
        return self.auth_state

    def order_callback(self, orders):
        raise NotImplementedError("Order Callback")

    def on_connect_realtime_ticks(self, *args, **kwargs):
        raise NotImplementedError("Order Callback")

    def on_close_realtime_ticks(self, *args, **kwargs):
        raise NotImplementedError("Order Callback")

    @abstractmethod
    def get_tick_data(self, instruments=None):
        pass

    @abstractmethod
    def get_orders(self) -> list[Order]:
        pass

    @abstractmethod
    def download_historic_data(self,
                               scrip:str,
                               exchange: str,
                               interval: str,
                               from_date: datetime.datetime,
                               to_date: datetime.datetime) -> bool:
        pass

    @abstractmethod
    def place_order(self,
                    order: Order):
        pass

    def load_tick_data_from_redis(self):
        self.tick_data = self.redis.json().get(f'kite-ticks-{self.today_timestamp()}', '$')
        if self.tick_data is None:
            self.logger.info("Tick data is empty")
            self.tick_data = {}
        if isinstance(self.tick_data, list):
            self.tick_data = self.tick_data[0]
    
    def store_tick_data_to_redis(self):
        self.redis.json().set(f'kite-ticks-{self.today_timestamp()}', '$', self.tick_data)

    def start_realtime_ticks(self, instruments: list, *args, **kwargs):
        self.load_tick_data_from_redis()
        self.start_realtime_ticks_impl(instruments, *args, **kwargs)

    @abstractmethod
    def start_realtime_ticks_impl(self, instruments: list, *args, **kwargs):
        pass

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
                for f in files:
                    scrip, exchange, fdate, tdate = f.split("-")
                    fdate = self.date_file_date_to_datetime(fdate)
                    tdate = self.date_file_date_to_datetime(tdate)
                    data = []
                    if ((fdate >= from_date and tdate <=to_date)
                        or (tdate >= from_date and tdate <= to_date)
                        or (fdate >= from_date and fdate <= to_date)):
                        data.append(self.read_data_file(os.path.join(dirname, f)))
                    return self.postprocess_data(pd.concat([d for d in data if len(d) > 0],
                                                             axis=0,
                                                             ignore_index=True),
                                                   interval)
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

    def get_tick_data_as_ohlc(self, from_redis: bool = True) -> list[pd.DataFrame]:
        if from_redis:
            self.load_tick_data_from_redis()
        dfs = {}
        for instrument_token, data in self.tick_data.items():
            df = pd.DataFrame.from_dict(data, orient='index')
            df.index = pd.to_datetime(df.index)
            dfs[instrument_token] = df
        return dfs
