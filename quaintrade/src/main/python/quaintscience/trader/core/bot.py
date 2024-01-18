from typing import Union, Optional
import datetime
import time
from functools import partial

import pandas as pd
import schedule
from tabulate import tabulate


from .ds import OHLCStorageType
from .util import resample_candle_data, get_key_from_scrip_and_exchange, new_id
from .logging import LoggerMixin
from .roles import Broker, HistoricDataProvider
from .strategy import Strategy
from .graphing import plot_backtesting_results

from ..integration.paper import PaperBroker
from ..integration.common import get_instruments_for_provider, get_instrument_for_provider


class Bot(LoggerMixin):

    def __init__(self,
                 broker: Broker,
                 strategy: Strategy,
                 data_provider: HistoricDataProvider,
                 *args,
                 live_data_context_size: int = 60,
                 online_mode: bool = False,
                 timeslot_offset_seconds: float = -2.0,
                 live_trading_market_start_hour: int = 9,
                 live_trading_market_start_minute: int = 15,
                 live_trading_market_end_hour: int = 15,
                 live_trading_market_end_minute: int = 30,
                 backtesting_print_tables: bool = True,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.broker = broker
        self.strategy = strategy
        self.data_provider = data_provider
        self.live_data_context_size = live_data_context_size
        self.online_mode = online_mode
        self.timeslot_offset_seconds = timeslot_offset_seconds
        self.live_trading_market_start_hour = live_trading_market_start_hour
        self.live_trading_market_end_hour = live_trading_market_end_hour
        self.live_trading_market_start_minute = live_trading_market_start_minute
        self.live_trading_market_end_minute = live_trading_market_end_minute
        self.backtesting_print_tables = backtesting_print_tables
        self.live_data_cache = {}

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

    def __get_live_data_cache(self, scrip: str,
                              exchange: str):
        key = get_key_from_scrip_and_exchange(scrip, exchange)
        if key in self.live_data_cache:
            return self.live_data_cache[key]
    
    def __set_live_data_cache(self, scrip: str,
                              exchange: str, data: pd.DataFrame):
        key = get_key_from_scrip_and_exchange(scrip, exchange)
        self.live_data_cache[key] = data

    def __get_context_data(self,
                           scrip: str,
                           exchange: str,
                           from_date: Union[str, datetime.datetime],
                           to_date: Union[str, datetime.datetime],
                           interval: Optional[str] = None,
                           blend_live_data: bool = False):
        to_date_day_begin = to_date.replace(hour=0, minute=0, second=0, microsecond=0)
        interval = self.strategy.default_interval if interval is None else interval
        if blend_live_data and self.online_mode:
            self.logger.info(f"Fetching latest data from data provider...")
            self.data_provider.download_historic_data(scrip=scrip,
                                                      exchange=exchange,
                                                      interval="1min",
                                                      from_date=to_date_day_begin,
                                                      to_date=to_date,
                                                      finegrained=True)
        data = self.__get_live_data_cache(scrip, exchange)
        
        if data is None or len(data) == 0:
            data = self.data_provider.get_data_as_df(scrip=scrip, exchange=exchange,
                                                    from_date=from_date, to_date=to_date,
                                                    interval=interval,
                                                    storage_type=OHLCStorageType.PERM,
                                                    download_missing_data=self.online_mode)
            data["date"] = data.index
            self.__set_live_data_cache(scrip,
                                       exchange,
                                       data)
        if blend_live_data:
            data_updates = self.data_provider.get_data_as_df(scrip=scrip, exchange=exchange,
                                                             from_date=to_date_day_begin,
                                                             to_date=to_date,
                                                             interval=interval,
                                                             storage_type=OHLCStorageType.PERM,
                                                             download_missing_data=False)
            data_updates["date"] = data_updates.index
            print(data_updates)    
            if len(data_updates) > 0:
                data = pd.concat([data_updates, data],
                                  axis=0,
                                  ignore_index=True,
                                  sort=False).drop_duplicates(["date"], keep='last')
        
        live_data = self.data_provider.get_data_as_df(scrip=scrip, exchange=exchange,
                                                        from_date=to_date_day_begin,
                                                        to_date=to_date,
                                                        interval=interval,
                                                        storage_type=OHLCStorageType.LIVE)

        live_data["date"] = live_data.index
        print(live_data)
        if len(data) == 0:
            self.logger.warn(f"Did not find historic data. Using only live data...")
            data = live_data
        elif len(live_data) > 0:
            self.logger.info(f"Found a combination of live and historic data")
            data = pd.concat([live_data, data],
                                axis=0,
                                ignore_index=True,
                                sort=False).drop_duplicates(["date"], keep='last')
        data = data.set_index(data["date"])
        data.drop(["date"], axis=1, inplace=True)
        data.sort_index(inplace=True)
        print(data)
        self.logger.info(f"First {data.iloc[0].name} - Latest {data.iloc[-1].name}")
        context = self.get_context(data)
        return context, data

    def pick_relevant_context(self, context: dict[str, pd.DataFrame],
                              now_tick: datetime.datetime):
        this_context = {}
        for k, v in context.items():
            if k in ["1d", "1w"]:
                this_context[k] = v[v.index < now_tick.replace(hour=0,
                                                               minute=0,
                                                               second=0,
                                                               microsecond=0)]
                continue
            this_context[k] = v[v.index < now_tick]
        return this_context

    def backtest(self,
                 scrip: str,
                 exchange: str,
                 from_date: Union[str, datetime.datetime],
                 to_date: Union[str, datetime.datetime],
                 interval: Optional[str] = None,
                 window_size: int = 5,
                 plot_results: bool = False,
                 clear_tradebook_for_scrip_and_exchange: bool = False):

        self.broker.run_id = new_id()
        self.broker.strategy = self.strategy.strategy_name
        self.broker.run_name = "backtest"
        self.broker.disable_state_persistence = True

        if clear_tradebook_for_scrip_and_exchange:
            self.broker.clear_tradebooks(scrip, exchange)
        if not isinstance(self.broker, PaperBroker):
            raise TypeError(f"Cannot backtest with Broker of type {type(self.broker)}; Need PaperBroker")
        data_provider_instrument = get_instrument_for_provider({"scrip": scrip,
                                                                "exchange": exchange},
                                                                self.data_provider.__class__)
        context, data = self.__get_context_data(scrip=data_provider_instrument["scrip"],
                                                exchange=data_provider_instrument["exchange"],
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

            this_context = self.pick_relevant_context(context, now_tick)
            self.do(window=window, context=this_context, scrip=scrip, exchange=exchange)
            if self.backtesting_print_tables:
                self.logger.info("--------------Tables After Strategy Computation Start-------------")
                self.broker.get_orders_as_table()
                self.broker.get_positions_as_table()
                self.logger.info("--------------Tables After Strategy Computation End-------------")

        self.broker.get_tradebook_storage().commit()

        if plot_results:
            storage = self.broker.get_tradebook_storage()
            positions = storage.get_positions_for_run(self.broker.strategy,
                                                      self.broker.run_name,
                                                      run_id=self.broker.run_id,
                                                      from_date=from_date,
                                                      to_date=to_date)
            positions = positions[(positions["scrip"] == scrip) & (positions["exchange"] == exchange)]
            data = data.merge(positions, on="date", how='left')

            data["pnl"].fillna(0., inplace=True)
            #import ipdb
            #ipdb.set_trace()
            daily_pnl = data["pnl"].resample('1d').apply('last').resample(interval).ffill().fillna(0.)
            data = data.merge(daily_pnl, how='left', left_index=True, right_index=True)
            data["daily_pnl"] = data["pnl_y"]
            data["pnl"] = data["pnl_x"]
            data.drop(["pnl_x", "pnl_y"], axis=1, inplace=True)

            monthly_pnl = data["pnl"].resample('1M').apply('last').resample(interval).ffill().fillna(0.)
            data = data.merge(monthly_pnl, how='left', left_index=True, right_index=True)
            data["monthly_pnl"] = data["pnl_y"].fillna(0.)
            data["pnl"] = data["pnl_x"]
            data.drop(["pnl_x", "pnl_y"], axis=1, inplace=True)
            print(data)

            self.strategy.plottables["indicator_fields"].append({"field": "pnl", "panel": 1})
            self.strategy.plottables["indicator_fields"].append({"field": "daily_pnl", "panel": 1})
            self.strategy.plottables["indicator_fields"].append({"field": "monthly_pnl", "panel": 1})
            events = storage.get_events(self.broker.strategy, self.broker.run_name, run_id=self.broker.run_id)
            events = events[(events["scrip"] == scrip) & (events["exchange"] == exchange)]
            plot_backtesting_results(data, context=context, interval=interval, events=events,
                                     indicator_fields=self.strategy.plottables["indicator_fields"])

    def get_trading_timeslots(self, interval):
        d = datetime.datetime.now().replace(hour=self.live_trading_market_start_hour,
                                            minute=self.live_trading_market_start_minute,
                                            second=0,
                                            microsecond=0)
        start_datetime = d

        x = 0; res =[]
        if interval.endswith("min"):
            interval = int(interval[:-3])
        elif interval.endswith("d"):
            interval = 24 * 60 * int(interval[:-1])
        elif interval.endswith("w"):
            interval = 24 * 60 * 7 * int(interval[:-1])
        else:
            raise ValueError(f"Dont know how to handle {interval}")
        while d + datetime.timedelta(minutes=x) < d.replace(hour=self.live_trading_market_end_hour,
                                                            minute=self.live_trading_market_end_minute):
            next_timeslot = d + datetime.timedelta(minutes=x) + datetime.timedelta(seconds=self.timeslot_offset_seconds)
            if next_timeslot > start_datetime:
                res.append(next_timeslot)
            x+= interval
        
        return res

    def print_pending_trading_timeslots(self):
        all_jobs = schedule.get_jobs()
        self.logger.debug(f"{datetime.datetime.now()}: Pending job status")
        print(tabulate([[str(x.next_run)] for x in all_jobs]), flush=True)

    def do_live_trade_task(self,
                           instruments: list[dict[str, str]],
                           interval: str,
                           running_for_timeslot: Optional[datetime.datetime] = None):
        self.logger.info(f"===== started for {running_for_timeslot} =====")
        self.broker.strategy = self.strategy.strategy_name
        self.broker.run_name = "live"
        self.broker.run_id = new_id()
        to_date = datetime.datetime.now().replace(hour=23, minute=59, second=0, microsecond=0)
        from_date = to_date - datetime.timedelta(days=self.live_data_context_size)
        from_date = from_date.replace(hour=0, minute=0, second=0, microsecond=0)
        print(f"Live trade task {from_date} {to_date}")
        data_provider_instruments = get_instruments_for_provider(instruments,
                                                                 self.data_provider.__class__)
        broker_instruments = get_instrument_for_provider(instruments, self.broker.__class__)
        for ii, instrument in enumerate(data_provider_instruments):
            scrip, exchange = instrument["scrip"], instrument["exchange"]
            context, data = self.__get_context_data(scrip=scrip,
                                                    exchange=exchange,
                                                    from_date=from_date,
                                                    to_date=to_date,
                                                    interval=interval,
                                                    blend_live_data=True)
            context = self.pick_relevant_context(context, datetime.datetime.now())
            self.do(window=data,
                    context=context,
                    scrip=broker_instruments[ii]["scrip"],
                    exchange=broker_instruments[ii]["exchange"])
        self.logger.info(f"===== ended for {running_for_timeslot} =====")

    def live(self,
             instruments: list[dict[str, str]],
             interval: Optional[str] = None):

        self.do_live_trade_task(instruments=instruments, interval=interval)
        for dt in self.get_trading_timeslots(interval):
                func = partial(self.do_live_trade_task,
                               instruments=instruments,
                               interval=interval,
                               running_for_timeslot=dt)
                run_name = f"run-{self.strategy.__class__.__name__}-at-{dt.strftime('%H:%M')}"
                schedule.every().day.at(dt.strftime("%H:%M:%S")).do(func).tag(run_name)

        self.print_pending_trading_timeslots()

        while True:
            schedule.run_pending()
            time.sleep(1)
