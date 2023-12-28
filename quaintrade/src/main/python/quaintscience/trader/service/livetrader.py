import datetime
import time

from tabulate import tabulate
import pandas as pd
import configargparse
import schedule

from ..core.reflection import dynamically_load_class
from .common import TradeManagerService



class LiveTraderService(TradeManagerService):

    def __init__(self,
                 strategy,
                 *args,
                 from_date=None,
                 to_date=None,
                 interval="3min",
                 check_time_sanity: bool = True,
                 **kwargs):
        kwargs["provider"] = "paper"
        kwargs["init"] = False
        super().__init__(*args, **kwargs)
        self.from_date = self.__correct(from_date)
        self.to_date = self.__correct(to_date)
        self.interval = interval
        self.strategy = strategy
        self.trade_manager.interval = interval
        self.trade_manager.historic_context_from = self.from_date
        self.trade_manager.historic_context_to = self.to_date
        self.check_time_sanity = check_time_sanity
        self.StrategyClass = dynamically_load_class(self.strategy)
        self.trade_manager.init()

    def __correct(self, dt):
        if isinstance(dt, str):
            try:
                dt = datetime.datetime.strptime(dt, "%Y%m%d %H:%M")
            except Exception:
                dt = datetime.datetime.strptime(dt, "%Y%m%d")
        return dt

    def get_datetimes(self):
        d = datetime.datetime.now().replace(hour=9, minute=15, second=0, microsecond=0)
        x = 0; res =[]
        interval = None
        if self.interval.endswith("min"):
            interval = int(self.interval[:-3])
        elif self.interval.endswith("d"):
            interval = 24 * 60 * int(self.interval[:-1])
        elif self.interval.endswith("w"):
            interval = 24 * 60 * 7 * int(self.interval[:-1])
        else:
            raise ValueError(f"Dont know how to handle {self.interval}")
        while d + datetime.timedelta(minutes=x) < d.replace(hour=15, minute=31):
            res.append(d + datetime.timedelta(minutes=x)); x+= interval
        return res


    def start(self):
        self.logger.info("Starting live trader...")
        for dt in self.get_datetimes():
                schedule.every().day.at(dt.strftime("%H:%M")).do(self.trade).tag(f"run-{self.strategy}-at-{dt.strftime('%H:%M')}")
        while True:
            all_jobs = schedule.get_jobs()
            print("Pending jobs....")
            print(tabulate([[str(x.next_run)] for x in all_jobs]))
            schedule.run_pending()
            time.sleep(30)

    
    def trade(self):
        dfs = self.trade_manager.get_redis_tick_data_as_ohlc(from_redis=True,
                                                             interval=self.interval)

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
            key = self.trade_manager.get_key_from_scrip(instrument["scrip"], instrument["exchange"])
            latest_data = dfs[key]
            if self.check_time_sanity:
                if latest_data.iloc[-1].name.to_pydatetime() - datetime.datetime.now() > self.interval:
                    raise ValueError(f"Time sanity check failed. So not trading. "
                                     f"Latest data for {key} is {latest_data.iloc[-1].name.to_pydatetime()}"
                                     f"; Current time is {datetime.datetime.now()}. This difference is > {self.interval}")
            df = pd.concat([df, latest_data], axis=0)
            self.strategy_executer.trade(df=df,
                                        context_df=df,
                                        context_sampling_interval=self.interval,
                                        stream=True)

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
