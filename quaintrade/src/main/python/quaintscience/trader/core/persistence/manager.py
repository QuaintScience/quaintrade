from abc import abstractmethod, ABC
from typing import Union, Optional
import datetime


class ManagerMixin():

    def __init__(self, *args, **kwargs):
        pass

    @abstractmethod
    def get_trade_service_providers(self, provider_type: Optional[str] = None):
        pass

    @abstractmethod
    def store_trade_service_provider(self,
                                     name: str,
                                     ProviderClass: str,
                                     auth_cache_filepath: str,
                                     storage_class: str = "quaintscience.trader.core.persistence.SqliteOHLCStorage",
                                     auth_credentials: Optional[dict] = None,                          
                                     custom_kwargs: Optional[dict] = None):
        pass

    @abstractmethod
    def get_strategies(self):
        pass

    @abstractmethod
    def store_strategy(self,
                       name: str,
                       StrategyClass: str,
                       strategy_kwargs: Optional[dict] = None):
        pass


    @abstractmethod
    def get_backtesting_templates(self):
        pass

    @abstractmethod
    def store_backtesting_template(self,
                                   name: str,
                                   data_provider_name: str,
                                   strategy_name: str,
                                   from_date: datetime.datetime,
                                   to_date: datetime.datetime,
                                   interval: str = "3min",
                                   refresh_orders_immediately_on_gtt_state_change: str = False,
                                   plot_results: str = False,
                                   window_size: int = 5,
                                   live_trading_mode: bool = False,
                                   clear_tradebook_for_scrip_and_exchange: bool = False):
        pass

    @abstractmethod
    def get_backtest_stats(self):
        pass

    @abstractmethod
    def store_backtest_stats(self,
                             backtest_template_name: str,
                             run_id: str,
                             start_time: datetime.datetime,
                             end_time: datetime.datetime,
                             result: str):
        pass

    @abstractmethod
    def get_live_traders(self):
        pass

    @abstractmethod
    def put_live_trader(self,
                        name: str,
                        data_provider_name: str,
                        broker_name: str,
                        data_context_size: int = 60,
                        online_mode: bool = True,
                        bot_custom_kwargs: Optional[dict] = None):
        pass

    @abstractmethod
    def get_live_trader_stats(self):
        pass

    @abstractmethod
    def store_live_trader_stats(self,
                              live_trader_name: str,
                              start_time: datetime.datetime,
                              end_time: datetime.datetime,
                              instruments: list[str]):
        pass
