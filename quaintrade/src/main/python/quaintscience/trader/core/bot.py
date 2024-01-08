from typing import Union, Optional
from collections import defaultdict
import datetime

import pandas as pd

from .ds import OHLCStorageType
from .util import resample_candle_data
from .logging import LoggerMixin
from .roles import Broker, HistoricDataProvider
from .strategy import Strategy
from .graphing import plot_backtesting_results

from ..integration.paper import PaperBroker


class Bot(LoggerMixin):

    def __init__(self,
                 broker: Broker,
                 strategy: Strategy,
                 data_provider: HistoricDataProvider,
                 *args,
                 live_data_context_size: int = 60,
                 online_mode: bool = False
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.broker = broker
        self.strategy = strategy
        self.data_provider = data_provider
        self.live_data_context_size = live_data_context_size
        self.online_mode = online_mode
    def do(self,
           scrip: str,
           exchange: str,
           context: dict[str, pd.DataFrame],
           window: pd.DataFrame):

        self.strategy.apply(window=window,
                            context=context,
                            broker=self.broker,
                            scrip=scrip,
                            exchange=exchange)

    def get_context(self, data: pd.DataFrame):

        data = self.strategy.compute_indicators(data)
        context = {}
        for ctx in self.strategy.context_required:
            ctx_data = resample_candle_data(data, ctx)
            ctx_data = self.strategy.indicator_pipeline.compute(ctx_data)[0]
            context[ctx] = ctx_data
        return context

    def __get_context_data(self,
                           scrip: str,
                           exchange: str,
                           from_date: Union[str, datetime.datetime],
                           to_date: Union[str, datetime.datetime],
                           interval: Optional[str] = None,
                           blend_live_data: bool = False):

        interval = self.strategy.default_interval if interval is None else interval
        data = self.data_provider.get_data_as_df(scrip=scrip, exchange=exchange,
                                                 from_date=from_date, to_date=to_date,
                                                 interval=interval,
                                                 storage_type=OHLCStorageType.PERM,
                                                 download_missing_data=self.online_mode)
        data["date"] = data.index
        if blend_live_data:
            live_data = self.data_provider.get_data_as_df(scrip=scrip, exchange=exchange,
                                                          from_date=from_date, to_date=to_date,
                                                          interval=interval,
                                                          storage_type=OHLCStorageType.LIVE)

            live_data["date"] = live_data.index
            if len(data) == 0:
                self.logger.warn(f"Did not find historic data. Using only live data...")
                data = live_data
            elif len(live_data) > 0:
                self.logger.info(f"Found a combination of live and historic data")
                data = pd.concat([data, live_data],
                                 axis=0,
                                 ignore_index=True,
                                 sort=False).drop_duplicates(["date"], keep='last')

        data.set_index(data["date"])
        data.drop(["date"], axis=1, inplace=True)
        context = self.get_context(data)
        return context, data

    def backtest(self,
                 scrip: str,
                 exchange: str,
                 from_date: Union[str, datetime.datetime],
                 to_date: Union[str, datetime.datetime],
                 interval: Optional[str] = None,
                 window_size: int = 5,
                 plot_results: bool = False):
        
        if not isinstance(self.broker, PaperBroker):
            raise TypeError(f"Cannot backtest with Broker of type {type(self.broker)}; Need PaperBroker")



        context, data = self.__get_context_data(scrip=scrip,
                                                exchange=exchange,
                                                from_date=from_date,
                                                to_date=to_date,
                                                interval=interval,
                                                blend_live_data=False)

        ts = None
        for ii in range(1, len(data) - window_size + 1, 1):
            window = data.iloc[ii: ii + window_size]
            if ts is None or ts.day != window.iloc[-1].name.day:
                self.logger.info(f"Trading on {window.iloc[-1].name.day}")
            ts = window.iloc[-1].name
            now_tick = window.iloc[-1].name.to_pydatetime()
            
            self.broker.set_current_time(now_tick, traverse=True)

            this_context = {}
            for k, v in context.items():
                if k in ["1d", "1w"]:
                    this_context[k] = v[v.index < now_tick.replace(hour=0,
                                                                   minute=0,
                                                                   second=0,
                                                                   microsecond=0)]
                    continue
                this_context[k] = v[v.index < now_tick]

            self.do(window=window, context=this_context, scrip=scrip, exchange=exchange)

            self.logger.info("--------------Tables After Strategy Computation Start-------------")
            self.broker.get_orders_as_table()
            self.broker.get_positions_as_table()
            self.logger.info("--------------Tables After Strategy Computation End-------------")
            
        if plot_results:
            pnl_history = self.broker.get_pnl_history_as_table()
            pnl_history = pd.DataFrame(pnl_history)
            pnl_history = pnl_history.set_index(pnl_history.columns[0])
            data["pnl_history"] = pnl_history[pnl_history.columns[0]]
            self.strategy.plottables["indicator_fields"].append({"field": "pnl_history", "panel": 1})
            plot_backtesting_results(data, events=self.broker.events,
                                     indicator_fields=self.strategy.plottables["indicator_fields"])

    def live(self,
             scrip: str,
             exchange: str,
             interval: Optional[str] = None):
        to_date = datetime.datetime.now().replace(second=0, microsecond=0)
        from_date = to_date - datetime.timedelta(days=self.live_data_context_size)

        context, data = self.__get_context_data(scrip=scrip,
                                                exchange=exchange,
                                                from_date=from_date,
                                                to_date=to_date,
                                                interval=interval,
                                                blend_live_data=True)
        self.do(window=data, context=context, scrip=scrip, exchange=exchange)
