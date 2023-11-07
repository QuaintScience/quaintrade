import time

import datetime
import configargparse

from ..integration.kite import KiteManager
from .common import TradeManagerService


class HistoricDataDownloader(TradeManagerService):

    def __init__(self,
                 *args, from_date=None, to_date=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.from_date = self.__correct(from_date)
        self.to_date = self.__correct(to_date)

    def __correct(self, dt):
        if isinstance(dt, str):
            try:
                dt = datetime.datetime.strptime(dt, "%Y%m%d %H:%M")
            except Exception:
                dt = datetime.datetime.strptime(dt, "%Y%m%d")
        return dt

    def start(self):
        self.logger.info("Getting historic data....")
        for instrument in self.instruments:
            print(instrument)
            self.logger.info(f"Downloading instrument {instrument}")
            self.manager.download_historic_data(scrip=instrument["scrip"],
                                                exchange=instrument["exchange"],
                                                interval="minute",
                                                from_date=self.from_date,
                                                to_date=self.to_date)

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
