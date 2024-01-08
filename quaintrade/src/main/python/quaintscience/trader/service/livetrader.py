import datetime
from configargparse import ArgParser
from typing import Union

from ..core.util import get_datetime
from .common import BotService, DataProviderService
from ..integration.paper import PaperBroker
from ..core.util import get_datetime


class LiveTraderService(BotService):

    default_config_file = ".live.trader.env"

    def __init__(self,
                 *args,
                 from_date: Union[str, datetime.datetime] = None,
                 to_date: Union[str, datetime.datetime] = None,
                 interval: str = "3min",
                 refresh_orders_immediately_on_gtt_state_change: bool = False,
                 plot_results: bool = False,
                 window_size: int = 5,
                 **kwargs):
        self.from_date = get_datetime(from_date)
        self.to_date = get_datetime(to_date)
        self.interval = interval
        self.plot_results = plot_results
        self.window_size = window_size
        kwargs["data_provider_login"] = False
        kwargs["data_provider_init"] = False
        DataProviderService.__init__(self, *args, **kwargs)
        kwargs["BrokerClass"] = PaperBroker
        kwargs["broker_login"] = False
        kwargs["broker_init"] = True
        kwargs["broker_custom_kwargs"] = {"instruments": self.instruments,
                                          "data_provider": self.data_provider,
                                          "historic_context_from": self.from_date,
                                          "historic_context_to": self.to_date,
                                          "interval": self.interval,
                                          "refresh_orders_immediately_on_gtt_state_change": refresh_orders_immediately_on_gtt_state_change}
        BotService.__init__(self,
                            *args,
                            **kwargs)
        

    def start(self):
        self.logger.info("Running backtest...")
        for instrument in self.instruments:
            self.bot.backtest(scrip=instrument["scrip"],
                              exchange=instrument["exchange"],
                              from_date=self.from_date,
                              to_date=self.to_date,
                              interval=self.interval,
                              window_size=self.window_size,
                              plot_results=self.plot_results)

    @classmethod
    def enrich_arg_parser(cls, p: ArgParser):
        BotService.enrich_arg_parser(p)
        p.add('--from_date', help="From date", env_var="FROM_DATE")
        p.add('--to_date', help="To date", env_var="TO_DATE")
        p.add('--interval', help="To date", env_var="INTERVAL")
        p.add('--refresh_orders_immediately_on_gtt_state_change',
              help="Refresh orders when gtt orders are executed",
              env_var="REFRESH_UPON_GTT_ORDERS")
        p.add('--plot_results', action="store_true", help="Plot backtesting results", env_var="PLOT_RESULTS")
        p.add('--window_size', type=int, help="Window size to be passed into backtesting function", env_var="WINDOW_SIZE")
