from ..core.graphing import live_ohlc_plot

import configargparse

from .common import TradeManagerService


class OHLCRealtimeGrapher(TradeManagerService):

    def __init__(self,
                 *args, 
                 interval="10m",
                 **kwargs):
        self.interval = interval
        super().__init__(*args, login_needed=False, **kwargs)

    def __get_live_data(self):
        return list(self.trade_manager.get_redis_tick_data_as_ohlc(from_redis=True).values())[0]

    def start(self):
        live_ohlc_plot(get_live_ohlc_func=self.__get_live_data)

    @staticmethod
    def get_args():

        p = configargparse.ArgParser(default_config_files=['.kite.env'])
        p.add('--redis_server', help="Redis server host", env_var="REDIS_SERVER")
        p.add('--redis_port', help="Redis server port", env_var="REDIS_PORT")
        p.add('--cache_path', help="Kite data cache path", env_var="CACHE_PATH")
        p.add('--instruments', help="Instruments in scrip:exchange,scrip:exchange format", env_var="INSTRUMENTS")
        return p.parse_known_args()


if __name__ == "__main__":
    listener = OHLCRealtimeGrapher()
    listener.start()
