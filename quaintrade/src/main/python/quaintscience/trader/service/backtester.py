import datetime
from configargparse import ArgParser
from typing import Union

from ..core.util import get_datetime
from .common import BotService, DataProviderService
from ..integration.paper import PaperBroker
from ..core.util import get_datetime


class BackTesterService(BotService):

    default_config_file = ".backtesting.trader.env"

    def __init__(self,
                 *args,
                 from_date: Union[str, datetime.datetime] = None,
                 to_date: Union[str, datetime.datetime] = None,
                 interval: str = "3min",
                 refresh_orders_immediately_on_gtt_state_change: bool = False,
                 plot_results: bool = False,
                 window_size: int = 5,
                 live_trading_mode: bool = False,
                 clear_tradebook_for_scrip_and_exchange: bool = False,
                 **kwargs):
        self.from_date = get_datetime(from_date)
        self.to_date = get_datetime(to_date)
        self.interval = interval
        self.plot_results = plot_results
        self.window_size = window_size
        self.live_trading_mode = live_trading_mode
        self.clear_tradebook_for_scrip_and_exchange = clear_tradebook_for_scrip_and_exchange
        print(kwargs)
        if self.live_trading_mode:
            kwargs["data_provider_login"] = True
            kwargs["data_provider_init"] = True
        else:
            kwargs["data_provider_login"] = False
            kwargs["data_provider_init"] = False
        DataProviderService.__init__(self, *args, **kwargs)
        kwargs["BrokerClass"] = PaperBroker
        kwargs["broker_login"] = False
        kwargs["broker_init"] = True
        kwargs["broker_skip_order_streamer"] = True
        broker_kwargs_overrides = {"instruments": self.instruments,
                                   "data_provider": self.data_provider,
                                   "historic_context_from": self.from_date,
                                   "historic_context_to": self.to_date,
                                   "interval": self.interval,
                                   "refresh_orders_immediately_on_gtt_state_change": refresh_orders_immediately_on_gtt_state_change,
                                   "refresh_data_on_every_time_change": False}
        if "broker_custom_kwargs" in kwargs and isinstance(kwargs["broker_custom_kwargs"], dict):
            kwargs["broker_custom_kwargs"].update(broker_kwargs_overrides)
        else:
            kwargs["broker_custom_kwargs"] = broker_kwargs_overrides
        print(kwargs["broker_custom_kwargs"])
        BotService.__init__(self,
                            *args,
                            **kwargs)

    def start(self):
        self.logger.info("Running backtest...")
        if self.live_trading_mode:
            self.bot.live(self.instruments,
                          self.interval)
        else:
            for instrument in self.instruments:
                self.bot.backtest(scrip=instrument["scrip"],
                                  exchange=instrument["exchange"],
                                  from_date=self.from_date,
                                  to_date=self.to_date,
                                  interval=self.interval,
                                  window_size=self.window_size,
                                  plot_results=self.plot_results,
                                  clear_tradebook_for_scrip_and_exchange=self.clear_tradebook_for_scrip_and_exchange)

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
        p.add('--live_trading_mode', action="store_true", help="Run bot in live mode with paper broker", env_var="LIVE_TRADING_MODE")
        p.add('--clear_tradebook_for_scrip_and_exchange', action="store_true", help="Clear tradebook for scrip and exchange", env_var="CLEAR_TRADEBOOK_FOR_SCRIP_AND_EXCHANGE")
