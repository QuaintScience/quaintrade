from abc import abstractmethod, ABC
from typing import Union, Optional
from collections import defaultdict
import datetime

import pandas as pd

from .ds import OHLCStorageType
from .util import resample_candle_data
from .logging import LoggerMixin
from .roles import Broker, DataProvider
from .strategy import Strategy
from .graphing import plot_backtesting_results

from ..integration.paper import PaperBroker


class Bot(ABC, LoggerMixin):

    def __init__(self,
                 broker: Broker,
                 strategy: Strategy,
                 *args,
                 **kwargs):
        self.broker = broker,
        self.strategy = strategy
        super().__init__(*args, **kwargs)

    @abstractmethod
    def do(self,
           context: dict[str, pd.DataFrame],
           window: pd.DataFrame):

        self.strategy.apply(window=window,
                            context=context,
                            broker=self.broker)

    def get_context(self, data: pd.DataFrame):

        data = self.strategy.compute_indicators(data)

        daily_context = resample_candle_data(data, "1d")
        daily_context = self.strategy.indicator_pipeline.compute(daily_context)[0]
        
        weekly_context = resample_candle_data(data, "1w")
        weekly_context = self.strategy.indicator_pipeline.compute(weekly_context)[0]
        
        hourly_context = resample_candle_data(data, "1h")
        hourly_context = self.strategy.indicator_pipeline.compute(hourly_context)[0]

        bihourly_context = resample_candle_data(data, "2h")
        bihourly_context = self.strategy.indicator_pipeline.compute(bihourly_context)[0]

        trihourly_context = resample_candle_data(data, "3h")
        trihourly_context = self.strategy.indicator_pipeline.compute(trihourly_context)[0]
        return {"1d": daily_context,
                "1w": weekly_context,
                "1h": hourly_context,
                "2h": bihourly_context,
                "3h": trihourly_context}

    def backtest(self,
                 data_provider: DataProvider,
                 scrip: str,
                 exchange: str,
                 from_date: Union[str, datetime.datetime],
                 to_date: Union[str, datetime.datetime],
                 interval: Optional[str] = None,
                 context_size: int = 5,
                 plot_results: bool = False):
        
        if not isinstance(self.broker, PaperBroker):
            raise TypeError(f"Cannot backtest with Broker of type {type(self.broker)}; Need PaperBroker")

        interval = self.strategy.default_interval if interval is None else interval
        data = data_provider.get_data_as_df(scrip=scrip, exchange=exchange,
                                            from_date=from_date, to_date=to_date,
                                            interval=interval,
                                            storage_type=OHLCStorageType.PERM)

        context = self.get_context(data)

        ts = None
        for ii in range(1, len(data) - context_size + 1, 1):
            window = data.iloc[ii: ii + context_size]
            
            if ts is None or ts.day != window.iloc[-1].name.day:
                self.logger.info(f"Trading on {window.iloc[-1].name.day}")

            ts = window.iloc[-1].name
            now_tick = window.iloc[-1].name.to_pydatetime()
            
            self.broker.set_current_time(now_tick, traverse=True)

            this_context = {}
            for k, v in context.items():
                this_context[k] = v[v.index <= now_tick]

            self.do(window=window, context=context)

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

    @abstractmethod
    def live(self,
             data_provider: DataProvider,
             scrip: str,
             exchange: str,
             context_from_date: Union[str, datetime.datetime],
             context_to_date: Union[str, datetime.datetime]):
        pass
