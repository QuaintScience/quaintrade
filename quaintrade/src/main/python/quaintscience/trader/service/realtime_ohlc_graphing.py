import asyncio
import os
from typing import Iterator
import time

import pandas as pd

import mplfinance as mpf
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.animation as animation


matplotlib.use('TkAgg')

import configargparse

from ..integration.kite import KiteManager
from .common import TradeManagerService


class OHLCRealtimeGrapher(TradeManagerService):

    def __init__(self,
                 *args, 
                 interval="10m",
                 **kwargs):
        self.interval = interval
        super().__init__(*args, login_needed=False, **kwargs)

    def animate(self, ival):
        df = list(self.trade_manager.get_redis_tick_data_as_ohlc(from_redis=True).values())[0]
        self.ax.clear()
        mpf.plot(df, ax=self.ax,type='candle', style=self.candlestick_style)

    def start(self):
        self.candlestick_style = mpf.make_mpf_style(base_mpf_style='yahoo', rc={'font.size': 6})
        self.fig, self.axes = mpf.plot(list(self.trade_manager.get_redis_tick_data_as_ohlc(from_redis=True).values())[0],
                                       returnfig=True,
                                       type='candle',
                                       title='Live Data',
                                       style=self.candlestick_style)
        self.ax = self.axes[0]
        ani = animation.FuncAnimation(self.fig, self.animate, interval=250)
        mpf.show()

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
