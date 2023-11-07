import os
import time

import configargparse

from .common import TradeManagerService


class RealtimeListener(TradeManagerService):

    def __init__(self,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)

    def start(self):
        self.logger.info("Starting stream....")
        self.manager.start_realtime_ticks(self.instruments)
        self.manager.ticker_thread.join()
        while True: time.sleep(1)

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
        p.add('--provider', help="Provider", env_var="PROVIDER", default="kite")
        p.add('--instruments', help="Instruments in scrip:exchange,scrip:exchange format", env_var="INSTRUMENTS")
        return p.parse_known_args()
