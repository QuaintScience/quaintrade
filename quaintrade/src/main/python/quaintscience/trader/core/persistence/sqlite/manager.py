from abc import abstractmethod, ABC
from typing import Union, Optional
import datetime
import json

from ..manager import ManagerMixin
from .common import SqliteStorage


class SqliteManager(SqliteStorage, ManagerMixin):

    def __init__(self, *args,
                 instance_name: str = "default",
                 **kwargs):
        self.instance_name = instance_name
        super().__init__(*args, **kwargs)

    def create_tables_impl(self, table_name, conflict_resolution_type: str = "REPLACE"):
        self.connection.execute(f"""CREATE TABLE IF NOT EXISTS {table_name}__data_providers (name VARCHAR(255) NOT NULL,
                                                                             ProviderClass VARCHAR(255) NOT NULL,
                                                                             auth_cache_filepath VARCHAR(255) NOT NULL,
                                                                             auth_credentials BLOB,
                                                                             StorageClass VARCHAR(255) NOT NULL,
                                                                             custom_kwargs BLOB,
                                                                             PRIMARY KEY (name) ON CONFLICT {conflict_resolution_type});""")
        
        self.connection.execute(f"""CREATE TABLE IF NOT EXISTS {table_name}__brokers (name VARCHAR(255) NOT NULL,
                                                                             ProviderClass VARCHAR(255) NOT NULL,
                                                                             auth_cache_filepath VARCHAR(255) NOT NULL,
                                                                             auth_credentials BLOB,
                                                                             TradingBookStorageClass VARCHAR(255) NOT NULL,
                                                                             run_name VARCHAR(255) NOT NULL,
                                                                             thread_id VARCHAR(255) NOT NULL,
                                                                             custom_kwargs BLOB,
                                                                             PRIMARY KEY (name) ON CONFLICT {conflict_resolution_type});""")

        self.connection.execute(f"""CREATE TABLE IF NOT EXISTS {table_name}__strategies (name VARCHAR(255) NOT NULL,
                                                                             StrategyClass VARCHAR(255) NOT NULL,
                                                                             custom_kwargs BLOB,
                                                                             PRIMARY KEY (name) ON CONFLICT {conflict_resolution_type});""")
    
        self.connection.execute(f"""CREATE TABLE IF NOT EXISTS {table_name}__bt_templates (name VARCHAR(255) NOT NULL,
                                                                             data_provider_name VARCHAR(255) NOT NULL,
                                                                             strategy_name VARCHAR(255) NOT NULL,
                                                                             from_date VARCHAR(255) NOT NULL,
                                                                             to_date VARCHAR(255) NOT NULL,
                                                                             interval VARCHAR(20) NOT NULL,
                                                                             refresh_orders_immediately_on_gtt_state_change INTEGER DEFAULT 0,
                                                                             plot_results INTEGER DEFAULT 0,
                                                                             window_size INTEGER DEFAULT 5,
                                                                             live_trading_mode INTEGER DEFAULT 0,
                                                                             clear_tradebook_for_scrip_and_exchange INTEGER DEFAULT 0,
                                                                             PRIMARY KEY (name) ON CONFLICT {conflict_resolution_type});""")

        self.connection.execute(f"""CREATE TABLE IF NOT EXISTS {table_name}__bt_stats (backtest_template_name VARCHAR(255) NOT NULL,
                                                                                       run_id VARCHAR(255) NOT NULL,
                                                                                       start_date VARCHAR(255) NOT NULL,
                                                                                       end_date VARCHAR(255) NOT NULL,
                                                                                       result VARCHAR(255) NOT NULL);""")
        
        self.connection.execute(f"""CREATE TABLE IF NOT EXISTS {table_name}__live_templates (name VARCHAR(255) NOT NULL,
                                                                             data_provider_name VARCHAR(255) NOT NULL,
                                                                             broker_name VARCHAR(255) NOT NULL,
                                                                             strategy_name VARCHAR(255) NOT NULL,
                                                                             interval VARCHAR(20) NOT NULL,
                                                                             data_context_size INTEGER DEFAULT 60,
                                                                             online_mode INTEGER DEFAULT 1,
                                                                             custom_kwargs BLOB,
                                                                             PRIMARY KEY (name) ON CONFLICT {conflict_resolution_type});""")

        self.connection.execute(f"""CREATE TABLE IF NOT EXISTS {table_name}__live_stats (live_trader_name VARCHAR(255) NOT NULL,
                                                                                         instruments BLOB,
                                                                                         run_id VARCHAR(255) NOT NULL,
                                                                                         start_date VARCHAR(255) NOT NULL,
                                                                                         end_date VARCHAR(255) NOT NULL,
                                                                                         result VARCHAR(255) NOT NULL);""")
        
        

    @abstractmethod
    def get_data_providers(self, provider_type: Optional[str] = None):
        return self.get_timestamped_data(self.instance_name, table_name_suffixes=["data_providers"],
                                         index_col="name",
                                         skip_time_stamps=True )

    @abstractmethod
    def store_data_provider(self,
                            name: str,
                            ProviderClass: str,
                            auth_cache_filepath: str,
                            StorageClass: str = "quaintscience.trader.core.persistence.SqliteOHLCStorage",
                            auth_credentials: Optional[dict] = None,                          
                            custom_kwargs: Optional[dict] = None):
        key = self.init_cache_for(self.instance_name,
                                  conflict_resolution_type="REPLACE")
        self.cache[key]["data_providers"].append({"name": name,
                                                  "ProviderClass": ProviderClass,
                                                  "auth_cache_filepath": auth_cache_filepath,
                                                  "StorageClass": StorageClass,
                                                  "auth_credentials": json.dumps(auth_credentials),
                                                  "custom_kwargs": json.dumps(custom_kwargs)})

    @abstractmethod
    def get_strategies(self):
        pass

    @abstractmethod
    def store_strategy(self,
                       name: str,
                       StrategyClass: str,
                       custom_kwargs: Optional[dict] = None):
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
                        custom_kwargs: Optional[dict] = None):
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
