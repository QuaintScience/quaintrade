import datetime
from functools import partial

import pandas as pd

from ..core.graphing import live_ohlc_plot
from ..core.ds import OHLCStorageType
from .common import DataProviderService


class OHLCRealtimeGrapher(DataProviderService):
    def __init__(self,
                 *args,
                 interval: str = "1min",
                 context_days: int = 1,
                 **kwargs):
        self.to_date = datetime.datetime.now()
        self.from_date = self.to_date - datetime.timedelta(days=context_days)
        self.interval = interval
        super().__init__(*args, **kwargs)
        
    def __get_live_data(self, instrument):
        self.to_date = datetime.datetime.now()
        live_data = self.data_provider.get_data_as_df(scrip=instrument["scrip"],
                                                      exchange=instrument["exchange"],
                                                      interval=self.interval,
                                                      from_date=self.from_date,
                                                      to_date=self.to_date,
                                                      storage_type=OHLCStorageType.LIVE)
        historic_data = self.data_provider.get_data_as_df(scrip=instrument["scrip"],
                                                          exchange=instrument["exchange"],
                                                          interval=self.interval,
                                                          from_date=self.from_date,
                                                          to_date=self.to_date,
                                                          storage_type=OHLCStorageType.PERM)
        if len(historic_data) == 0:
            data = pd.DataFrame(columns=["open", "high", "low", "close"], index=pd.DatetimeIndex([datetime.datetime.now()]), data=[{"open": 0., "high": 0., "low": 0., "close": 0.}])
        else:
            data = pd.concat([live_data, historic_data], axis=0) if len(live_data) > 0 else historic_data
        return data

    def start(self):
        live_ohlc_plot(get_live_ohlc_func=partial(self.__get_live_data,
                                                  instrument=self.instruments[0]))

    @classmethod
    def get_arg_parser(cls):
        p = DataProviderService.get_arg_parser()
        p._default_config_files = [".historic.trader.env"]
        p.add('--plotting_interval', help="Plotting Interval", env_var="PLOTTING_INTERVAL")
        return p
