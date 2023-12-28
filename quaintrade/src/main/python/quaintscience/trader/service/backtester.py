import time
import copy
import datetime
import configargparse

from ..core.reflection import dynamically_load_class
from ..integration.kite import KiteManager
#from ..strategies.donchain_pullback import DonchainPullbackStrategy
#from ..strategies.wma_support import WMASupportStrategy
from ..strategies.hiekinashi import HiekinAshiStrategy
from .common import TradeManagerService
from ..core.util import get_datetime

class BackTesterService(TradeManagerService):

    def __init__(self,
                 strategy,
                 *args,
                 from_date=None,
                 to_date=None,
                 interval="3min",
                 **kwargs):
        kwargs["provider"] = "paper"
        kwargs["init"] = False
        super().__init__(*args, **kwargs)
        self.from_date = get_datetime(from_date)
        self.to_date = get_datetime(to_date)
        self.interval = interval
        self.strategy = strategy
        self.trade_manager.interval = interval
        self.trade_manager.historic_context_from = self.from_date
        self.trade_manager.historic_context_to = self.to_date
        self.StrategyClass = dynamically_load_class(self.strategy)
        self.trade_manager.init()

    def start(self):
        self.logger.info("Running backtest...")
        for instrument in self.instruments:
            self.strategy_executer = self.StrategyClass(signal_scrip=instrument["scrip"],
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
                                         context_df=df,
                                         context_sampling_interval=self.interval,
                                         stream=False)

    @staticmethod
    def get_args():

        p = configargparse.ArgParser(default_config_files=['.trader.env'])
        p.add('--api_key', help="API key", env_var="API_KEY")
        p.add('--api_secret', help="API secret", env_var="API_SECRET")
        p.add('--request_token', help="Request token (if first time login)", env_var="REQUEST_TOKEN")
        p.add('--access_token', help="Access token (repeat access)", env_var="ACCESS_TOKEN")
        p.add('--redis_server', help="Redis server host", env_var="REDIS_SERVER")
        p.add('--redis_port', help="Redis server port", env_var="REDIS_PORT")
        p.add('--cache_path', help="Data cache path", env_var="CACHE_PATH")
        p.add('--provider', help="Provider", env_var="PROVIDER")
        p.add('--from_date', help="From date", env_var="FROM_DATE")
        p.add('--to_date', help="To date", env_var="TO_DATE")
        p.add('--interval', help="Interval", env_var="INTERVAL")
        p.add('--strategy', help="Strategy class", env_var="STRATEGY")
        p.add('--instruments', help="Instruments in scrip:exchange,scrip:exchange format", env_var="INSTRUMENTS")
        return p.parse_known_args()
