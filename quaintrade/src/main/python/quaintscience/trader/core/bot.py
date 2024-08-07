from typing import Union, Optional
import datetime
import time
import traceback
import os
import traceback
import os
from functools import partial

import pandas as pd
import schedule
from tabulate import tabulate


from .ds import OHLCStorageType, TradingProduct
from .util import resample_candle_data, get_key_from_scrip_and_exchange, new_id, datestring_to_datetime
from .logging import LoggerMixin
from .roles import Broker, HistoricDataProvider
from .strategy import Strategy
from .graphing import plot_backtesting_results

from ..integration.paper import PaperBroker, PaperTraderTimeExceededException
from ..integration.common import get_instruments_for_provider, get_instrument_for_provider


class Bot(LoggerMixin):

    def __init__(self,
                 broker: Broker,
                 strategy: Strategy,
                 data_provider: HistoricDataProvider,
                 *args,
                 live_data_context_size: int = 60,
                 online_mode: bool = False,
                 timeslot_offset_seconds: float = -1.0,
                 live_trading_market_start_hour: int = 9,
                 live_trading_market_start_minute: int = 15,
                 live_trading_market_end_hour: int = 15,
                 live_trading_market_end_minute: int = 30,
                 backtesting_print_tables: bool = True,
                 backtest_results_folder: str = "backtest-results",
                 backtest_type: str = "standard",
                 backtest_display_data_only: bool = False,
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
        self.backtest_results_folder = backtest_results_folder
        self.backtest_type = backtest_type
        self.one_time_download_done = False
        self.backtest_display_data_only = backtest_display_data_only
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

    def get_context(self,
                    data: pd.DataFrame,
                    interval: str):
        
        rsdata = resample_candle_data(data, interval)
        context = {}
        if self.strategy is not None:
            rsdata = self.strategy.indicator_pipeline["window"].compute(rsdata)[0]
            for ctx, pipeline in self.strategy.indicator_pipeline["context"].items():
                ctx_data = resample_candle_data(data, ctx)
                ctx_data = pipeline.compute(ctx_data)[0]
                context[ctx] = ctx_data
        return rsdata, context

    def __get_live_data_cache(self, scrip: str,
                              exchange: str):
        key = get_key_from_scrip_and_exchange(scrip, exchange)
        if key in self.live_data_cache:
            return self.live_data_cache[key]
    
    def __set_live_data_cache(self, scrip: str,
                              exchange: str, data: pd.DataFrame):
        key = get_key_from_scrip_and_exchange(scrip, exchange)
        self.live_data_cache[key] = data

    def __one_time_context_download(self,
                                    scrip: str,
                                    exchange: str,
                                    to_date: Union[str, datetime.datetime]):
        to_date = to_date.replace(hour=self.live_trading_market_end_hour,
                                  minute=self.live_trading_market_end_minute,
                                  second=0,
                                  microsecond=0)
        from_date = to_date - datetime.timedelta(days=7)
        from_date = from_date.replace(hour=self.live_trading_market_start_hour,
                                      minute=self.live_trading_market_start_minute)
        if self.online_mode and not self.one_time_download_done:
            self.logger.info("Fetching one-time latest data from data provider...")
            self.data_provider.download_historic_data(scrip=scrip,
                                                      exchange=exchange,
                                                      interval="1min",
                                                      from_date=from_date,
                                                      to_date=to_date,
                                                      finegrained=True)
            self.one_time_download_done = True

    def __one_time_context_download(self,
                                    scrip: str,
                                    exchange: str,
                                    to_date: Union[str, datetime.datetime]):
        to_date = to_date.replace(hour=self.live_trading_market_end_hour,
                                  minute=self.live_trading_market_end_minute,
                                  second=0,
                                  microsecond=0)
        from_date = to_date - datetime.timedelta(days=7)
        from_date = from_date.replace(hour=self.live_trading_market_start_hour,
                                      minute=self.live_trading_market_start_minute)
        if self.online_mode and not self.one_time_download_done:
            self.logger.info("Fetching one-time latest data from data provider...")
            self.data_provider.download_historic_data(scrip=scrip,
                                                      exchange=exchange,
                                                      interval="1min",
                                                      from_date=from_date,
                                                      to_date=to_date,
                                                      finegrained=True)
            self.one_time_download_done = True

    def __get_context_data(self,
                           scrip: str,
                           exchange: str,
                           from_date: Union[str, datetime.datetime],
                           to_date: Union[str, datetime.datetime],
                           interval: Optional[str] = None,
                           blend_live_data: bool = False,
                           prefer_live_data: bool = False):
        to_date_day_begin = to_date.replace(hour=self.live_trading_market_start_hour,
                                            minute=self.live_trading_market_start_minute,
                                            second=0,
                                            microsecond=0)
        self.__one_time_context_download(scrip=scrip,
                                         exchange=exchange,
                                         to_date=to_date_day_begin)
                        #    blend_live_data: bool = False,
                        #    prefer_live_data: bool = False):
        to_date_day_begin = to_date.replace(hour=self.live_trading_market_start_hour,
                                            minute=self.live_trading_market_start_minute,
                                            second=0,
                                            microsecond=0)
        self.__one_time_context_download(scrip=scrip,
                                         exchange=exchange,
                                         to_date=to_date_day_begin)
        interval = self.strategy.default_interval if interval is None else interval
        if blend_live_data and self.online_mode:
            self.logger.info("Fetching latest data from data provider...")
            self.logger.info("Fetching latest data from data provider...")
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
                                                    interval="1min",
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
                                                             interval="1min",
                                                             storage_type=OHLCStorageType.PERM,
                                                             download_missing_data=False)
            data_updates["date"] = data_updates.index
            print("Data updates using historic data provider")
            print(data_updates)
            if data is None:
                data = data_updates
            elif len(data_updates) > 0:
                data = pd.concat([data, data_updates],
                                  axis=0,
                                  ignore_index=True,
                                  sort=False).drop_duplicates(["date"], keep='last')
            print("Data updates using historic data provider")
            print(data_updates)
            if data is None:
                data = data_updates
            elif len(data_updates) > 0:
                data = pd.concat([data, data_updates],
                                  axis=0,
                                  ignore_index=True,
                                  sort=False).drop_duplicates(["date"], keep='last')
        
        live_data = self.data_provider.get_data_as_df(scrip=scrip, exchange=exchange,
                                                        from_date=to_date_day_begin,
                                                        to_date=to_date,
                                                        interval="1min",
                                                        storage_type=OHLCStorageType.LIVE)

        live_data["date"] = live_data.index
        print("Live data")
        print("Live data")
        print(live_data)
        if len(data) == 0:
            self.logger.warn(f"Did not find historic data. Using only live data...")
            data = live_data
        elif len(live_data) > 0:
            self.logger.info(f"Found a combination of live and historic data")
            data = pd.concat([live_data, data],
                             axis=0,
                             ignore_index=True,
                             sort=False).drop_duplicates(["date"],
                                                         keep='last')
            
            if prefer_live_data:
                print("Preferring live data for last slot")
                data = pd.concat([data,
                                  live_data.iloc[[-1]]],
                                  axis=0,
                                  ignore_index=True,
                                  sort=False).drop_duplicates(subset='date', keep='last')
            
            if prefer_live_data:
                print("Preferring live data for last slot")
                data = pd.concat([data,
                                  live_data.iloc[[-1]]],
                                  axis=0,
                                  ignore_index=True,
                                  sort=False).drop_duplicates(subset='date', keep='last')

        data = data.set_index(data["date"])
        data.drop(["date"], axis=1, inplace=True)
        data.sort_index(inplace=True)
        print("Final data")
        print("Final data")
        print(data)
        if len(data) > 0:
            self.logger.info(f"First {data.iloc[0].name} - Latest {data.iloc[-1].name}")
        data, context = self.get_context(data, interval)
        #data = resample_candle_data(data, interval)
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
            this_context[k] = v[v.index < now_tick - datetime.timedelta(seconds=pd.Timedelta(k).total_seconds())]
            this_context[k] = v[v.index < now_tick - datetime.timedelta(seconds=pd.Timedelta(k).total_seconds())]
        return this_context

    def backtest(self,
                 scrip: str,
                 exchange: str,
                 from_date: Union[str, datetime.datetime],
                 to_date: Union[str, datetime.datetime],
                 context_from_date: Optional[Union[str, datetime.datetime]] = None,
                 interval: Optional[str] = None,
                 window_size: int = 5,
                 plot_results: bool = False,
                 clear_tradebook_for_scrip_and_exchange: bool = False):
        if context_from_date is None:
            context_from_date = from_date
        if context_from_date is None:
            context_from_date = from_date
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

        context, data = None, None
        if self.backtest_type == "standard":
            self.logger.info(f"Standard back test")
            context, data = self.__get_context_data(scrip=data_provider_instrument["scrip"],
                                                    exchange=data_provider_instrument["exchange"],
                                                    from_date=context_from_date,
                                                    to_date=to_date,
                                                    interval=interval,
                                                    blend_live_data=False)

            print("Backtest Data")
            print(data)
            print("Context")
            print(context)
            ts = None
            first_timeset_done = False

            if not self.backtest_display_data_only:

                if isinstance(from_date, str):
                    from_date = datestring_to_datetime(from_date)
                start_ii = data.index.get_indexer([from_date], method="nearest")[0]

                for ii in range(start_ii, len(data) - window_size + 1, 1):
                    window = data.iloc[ii: ii + window_size]
                    if ts is None or ts.day != window.iloc[-1].name.day:
                        self.logger.info(f"Trading on {window.iloc[-1].name.day}")
                    ts = window.iloc[-1].name
                    now_tick = window.iloc[-1].name.to_pydatetime()
                    prev_tick = window.iloc[-2].name.to_pydatetime()
                    if not first_timeset_done:
                        self.broker.set_current_time(prev_tick, traverse=False)
                        first_timeset_done = True
                    try:
                        this_context = self.pick_relevant_context(context, now_tick)
                        self.logger.info(f"Time now is {now_tick}; "
                                        f"last-data point is at {prev_tick}")
                        context_empty = False
                        for k, v in this_context.items():
                            if len(v) == 0:
                                context_empty = True
                                break
                            print(f"{now_tick} {k} last tick: {v.iloc[-1].name}")
                        if context_empty:
                            continue
                        self.do(window=window[:-1], context=this_context, scrip=scrip, exchange=exchange)                    
                        if self.backtesting_print_tables:
                            self.logger.info(f"--------------Tables After Strategy Computation Start {now_tick}-------------")
                            self.broker.get_orders_as_table()
                            self.broker.get_positions_as_table()
                            self.logger.info(f"--------------Tables After Strategy Computation End {now_tick}-------------")

                        self.logger.info(f"--------------Start Broker Activity for {now_tick} -------------")
                    
                        self.broker.set_current_time(now_tick, traverse=True)
                        self.logger.info(f"--------------End Broker Activity for {now_tick} -------------")

                    except PaperTraderTimeExceededException:
                        self.logger.warn(f"Could not set time in paper broker to {now_tick}")
        elif self.backtest_type == "live_simulation":
            self.logger.info(f"Live simulation backtest")
            if not self.backtest_display_data_only:
                timeslots = self.get_trading_timeslots(interval,
                                                    d=to_date - datetime.timedelta(days=1))

                self.broker.set_current_time(timeslots[0][1], traverse=False)
                for timeslot, exec_time  in timeslots:
                    print(f"============== Start {timeslot} ================")
                    from_date = to_date - datetime.timedelta(days=self.live_data_context_size)
                    from_date = from_date.replace(hour=0, minute=0, second=0, microsecond=0)
                    context, data = self.__get_context_data(scrip=data_provider_instrument["scrip"],
                                                            exchange=data_provider_instrument["exchange"],
                                                            from_date=from_date,
                                                            to_date=timeslot,
                                                            interval=interval,
                                                            blend_live_data=True,
                                                            prefer_live_data=True)
                    print(f"timeslot {timeslot} data")
                    print(data)
                    this_context = self.pick_relevant_context(context, timeslot)
                    try:
                        self.broker.set_current_time(exec_time, traverse=True)
                    except PaperTraderTimeExceededException:
                        self.logger.warn(f"Could not set time in paper broker to {exec_time}")
                        continue
                    self.do(window=data,
                            context=this_context,
                            scrip=scrip,
                            exchange=exchange)
                    if self.backtesting_print_tables:
                        self.logger.info("--------------Tables After Strategy Computation Start-------------")
                        self.broker.get_orders_as_table()
                        self.broker.get_positions_as_table()
                        self.logger.info("--------------Tables After Strategy Computation End-------------")
                    print(f"============== End {timeslot} ================")

        if not self.backtest_display_data_only:
            self.broker.get_tradebook_storage().commit()

            self.logger.info("===================== Stats ========================")
            pnl_data = []
            for k, v in self.broker.trade_pnl.items():
                pnl_data.append([k,
                                self.broker.trade_timestamps[k][0],
                                self.broker.trade_timestamps[k][1],
                                self.broker.trade_transaction_types[k],
                                v])
            cnts = [1 for v in self.broker.trade_pnl.values() if v > 0]
            if len(cnts) > 0:
                cnts = sum(cnts)
            else:
                cnts = 0
            accuracy = 0.
            if len(self.broker.trade_pnl) > 0:
                accuracy = cnts / len(self.broker.trade_pnl)
            max_drawdown = 0.
            curr_drawdown = 0.
            running_sum = 0.
            lowest_point = 0.
            max_profit_streak = 0
            max_loss_streak = 0
            loss_streak = 0
            profit_streak = 0
            for k, v in self.broker.trade_pnl.items():
                running_sum += v
                lowest_point = min(running_sum, lowest_point)
                if v > 0:
                    if curr_drawdown < 0:
                        max_drawdown = min(max_drawdown, curr_drawdown)
                        max_loss_streak = max(loss_streak, max_loss_streak)
                        curr_drawdown = 0.
                        loss_streak = 0
                    profit_streak += 1
                else:
                    curr_drawdown += v
                    loss_streak += 1
                    max_profit_streak = max(profit_streak, max_profit_streak)
                    profit_streak = 0
            if curr_drawdown < 0:
                max_drawdown = min(max_drawdown, curr_drawdown)
            max_profit_streak = max(profit_streak, max_profit_streak)
            max_loss_streak = max(loss_streak, max_loss_streak)
            os.makedirs(self.backtest_results_folder, exist_ok=True)
            fname = f"backtest-{scrip}:{exchange}-{self.strategy.__class__.__name__}-{interval}-{from_date.strftime('%Y%m%d')}-{to_date.strftime('%Y%m%d')}.txt"
            with open(os.path.join(self.backtest_results_folder, fname), 'w') as fid:
                print(tabulate(pnl_data, headers=["order_id", "entry_time", "exit_time", "pnl"]), file=fid)
                print(f"Found {len(self.broker.trade_pnl)} trades.", file=fid)
                print(f"Accuracy: {accuracy}", file=fid)
                print(f"Max Drawdown: {max_drawdown}", file=fid)
                print(f"Lowest point: {lowest_point}", file=fid)
                print(f"Longest Loss Streak: {max_loss_streak}", file=fid)
                print(f"Longest Profit Streak: {max_profit_streak}", file=fid)
                print(f"Final Pnl: {running_sum}", file=fid)
                print(f"Largest loss: {min(self.broker.trade_pnl.values()) if len(self.broker.trade_pnl) > 0 else 0}", file=fid)
            print(tabulate(pnl_data, headers=["order_id", "entry_time", "exit_time", "pnl"]))
            self.logger.info(f"Found {len(self.broker.trade_pnl)} trades.")
            self.logger.info(f"Accuracy: {accuracy}")
            self.logger.info(f"Max Drawdown: {max_drawdown}")
            self.logger.info(f"Lowest point: {lowest_point}")
            self.logger.info(f"Longest Loss Streak: {max_loss_streak}")
            self.logger.info(f"Longest Profit Streak: {max_profit_streak}")
            self.logger.info(f"Final Pnl: {running_sum}")
            self.logger.info(f"Largest loss: {min(self.broker.trade_pnl.values()) if len(self.broker.trade_pnl) > 0 else 0}")

        if plot_results or self.backtest_display_data_only:
            if not self.backtest_display_data_only:
                storage = self.broker.get_tradebook_storage()
                positions = storage.get_positions_for_run(self.broker.strategy,
                                                        self.broker.run_name,
                                                        run_id=self.broker.run_id,
                                                        from_date=from_date,
                                                        to_date=to_date)
                if positions is not None:
                    positions = positions[(positions["scrip"] == scrip) & (positions["exchange"] == exchange)]
                    data = data.merge(positions, on="date", how='left')

                    data["pnl"].fillna(0., inplace=True)
                    #import ipdb
                    #ipdb.set_trace()
                    daily_pnl = data["pnl"].resample('1d').apply('last').resample(interval,
                                                                                origin=datetime.datetime.fromisoformat('1970-01-01 09:15:00')).ffill().fillna(0.)
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
                if events is not None:
                    events = events[(events["scrip"] == scrip) & (events["exchange"] == exchange)]
                plot_backtesting_results(data, context=context, interval=interval, events=events,
                                        indicator_fields=self.strategy.plottables["indicator_fields"],
                                        plot_contexts=self.strategy.plot_context_candles,
                                        mpf_custom_kwargs=self.strategy.custom_plot_kwargs)
            else:
                for entry in self.strategy.plottables["indicator_fields"]:
                    if entry["panel"] > 0:
                        entry["panel"] -= 1
                plot_backtesting_results(data, context=context, interval=interval, events=None,
                                        indicator_fields=self.strategy.plottables["indicator_fields"],
                                        plot_contexts=self.strategy.plot_context_candles,
                                        mpf_custom_kwargs=self.strategy.custom_plot_kwargs)

    def get_trading_timeslots(self,
                              interval,
                              d: Optional[datetime.datetime] = None):
        if d is None:
            d = datetime.datetime.now()
        d = d.replace(hour=self.live_trading_market_start_hour,
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
            next_timeslot = d + datetime.timedelta(minutes=x)
            next_exectime = next_timeslot + datetime.timedelta(seconds=self.timeslot_offset_seconds)
            next_timeslot = next_timeslot.replace(second=0) - datetime.timedelta(seconds=1)
            if next_timeslot > start_datetime:
                res.append((next_timeslot, next_exectime))
            x+= interval
        
        return res

    def print_pending_trading_timeslots(self):
        all_jobs = schedule.get_jobs()
        self.logger.debug(f"{datetime.datetime.now()}: Pending job status")
        print(tabulate([[str(x.next_run)] for x in all_jobs]), flush=True)

    def get_recent_data(self, instruments, interval="1min"):
        to_date = datetime.datetime.now().replace(second=0, microsecond=0)
        from_date = to_date - datetime.timedelta(days=self.live_data_context_size)
        from_date = from_date.replace(hour=0, minute=0, second=0, microsecond=0)
        print(f"Get data {from_date} {to_date}")
        data_provider_instruments = get_instruments_for_provider(instruments,
                                                                 self.data_provider.__class__)
        ret_data = {}
        for ii, instrument in enumerate(data_provider_instruments):
            scrip, exchange = instrument["scrip"], instrument["exchange"]
            context, data = self.__get_context_data(scrip=scrip,
                                                    exchange=exchange,
                                                    from_date=from_date,
                                                    to_date=to_date,
                                                    interval=interval,
                                                    blend_live_data=True)
            context = self.pick_relevant_context(context, datetime.datetime.now())
            ret_data[f"{scrip}:{exchange}"] = {"context": context, "data": data}
        return ret_data


    def do_live_trade_task(self,
                           instruments: list[dict[str, str]],
                           interval: str,
                           running_for_timeslot: Optional[datetime.datetime] = None):
        self.logger.info(f"===== started for {running_for_timeslot} =====")
        self.broker.strategy = self.strategy.strategy_name
        self.broker.run_name = "live"
        self.broker.run_id = new_id()
        #to_date = datetime.datetime.now().replace(hour=23, minute=59, second=0, microsecond=0)
        to_date = running_for_timeslot
        #to_date = datetime.datetime.now().replace(hour=23, minute=59, second=0, microsecond=0)
        to_date = running_for_timeslot
        from_date = to_date - datetime.timedelta(days=self.live_data_context_size)
        from_date = from_date.replace(hour=0, minute=0, second=0, microsecond=0)
        print(f"Live trade task {from_date} {to_date}")
        data_provider_instruments = get_instruments_for_provider(instruments,
                                                                 self.data_provider.__class__)
        broker_instruments = get_instrument_for_provider(instruments, self.broker.__class__)
        self.broker.gtt_order_callback(refresh_cache=True)
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

        for timeslot, exectime in self.get_trading_timeslots(interval):
                func = partial(self.do_live_trade_task,
                               instruments=instruments,
                               interval=interval,
                               running_for_timeslot=timeslot)
                run_name = f"run-{self.strategy.__class__.__name__}-at-{exectime.strftime('%H:%M')}"
                schedule.every().day.at(exectime.strftime("%H:%M:%S")).do(func).tag(run_name)
                
        self.print_pending_trading_timeslots()

        while True:
            schedule.run_pending()
            time.sleep(1)
