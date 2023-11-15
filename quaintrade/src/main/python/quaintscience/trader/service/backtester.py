import time

import datetime
import configargparse

from ..integration.kite import KiteManager
from ..strategies.donchain_breakout import DonchainBreakoutStrategy
from .common import TradeManagerService


class BackTesterService(TradeManagerService):

    def __init__(self,
                 *args,
                 from_date=None,
                 to_date=None,
                 interval="10min",
                 **kwargs):
        kwargs["provider"] = "paper"
        kwargs["init"] = False
        super().__init__(*args, **kwargs)
        self.from_date = self.__correct(from_date)
        self.to_date = self.__correct(to_date)
        self.interval = interval
        self.trade_manager.interval = interval
        self.trade_manager.historic_context_from = self.from_date
        self.trade_manager.historic_context_to = self.to_date
        self.trade_manager.init()

    def __correct(self, dt):
        if isinstance(dt, str):
            try:
                dt = datetime.datetime.strptime(dt, "%Y%m%d %H:%M")
            except Exception:
                dt = datetime.datetime.strptime(dt, "%Y%m%d")
        return dt

    def start(self):
        self.logger.info("Running backtest...")
        for instrument in self.instruments:
            print(instrument)
            self.strategy_executer = DonchainBreakoutStrategy(signal_scrip=instrument["scrip"],
                                                              long_scrip=instrument["scrip"],
                                                              short_scrip=instrument["scrip"],
                                                              exchange=instrument["exchange"],
                                                              trade_manager=self.trade_manager)
            df = self.trade_manager.get_historic_data(scrip=instrument["scrip"],
                                                      exchange=instrument["exchange"],
                                                      interval=self.interval,
                                                      from_date=self.from_date,
                                                      to_date=self.to_date,
                                                      download=False)
            self.strategy_executer.trade(df=df,
                                         stream=False)

    @staticmethod
    def get_args():

        p = configargparse.ArgParser(default_config_files=['.kite.env'])
        p.add('--api_key', help="API key", env_var="API_KEY")
        p.add('--api_secret', help="API secret", env_var="API_SECRET")
        p.add('--request_token', help="Request token (if first time login)", env_var="REQUEST_TOKEN")
        p.add('--access_token', help="Access token (repeat access)", env_var="ACCESS_TOKEN")
        p.add('--redis_server', help="Redis server host", env_var="REDIS_SERVER")
        p.add('--redis_port', help="Redis server port", env_var="REDIS_PORT")
        p.add('--cache_path', help="Data cache path", env_var="CACHE_PATH")
        p.add('--provider', help="Provider", env_var="CACHE_PATH")
        p.add('--from_date', help="From date", env_var="FROM_DATE")
        p.add('--to_date', help="To date", env_var="TO_DATE")
        p.add('--instruments', help="Instruments in scrip:exchange,scrip:exchange format", env_var="INSTRUMENTS")
        return p.parse_known_args()
