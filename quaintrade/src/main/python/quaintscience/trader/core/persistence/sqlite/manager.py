import datetime
import json
from typing import Optional, Union

from ..manager import ManagerMixin
from .common import SqliteStorage
from ...util import get_datetime


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
                                                                             run_name VARCHAR(255),
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
                                                                             custom_kwargs BLOB,
                                                                             PRIMARY KEY (name) ON CONFLICT {conflict_resolution_type});""")

        self.connection.execute(f"""CREATE TABLE IF NOT EXISTS {table_name}__bt_stats (date VARCHAR(255) NOT NULL,
                                                                                       backtest_template_name VARCHAR(255) NOT NULL,
                                                                                       run_id VARCHAR(255) NOT NULL,
                                                                                       start_date VARCHAR(255) NOT NULL,
                                                                                       end_date VARCHAR(255) NOT NULL,
                                                                                       result BLOB);""")
        
        self.connection.execute(f"""CREATE TABLE IF NOT EXISTS {table_name}__live_templates (name VARCHAR(255) NOT NULL,
                                                                                             data_provider_name VARCHAR(255) NOT NULL,
                                                                                             broker_name VARCHAR(255) NOT NULL,
                                                                                             strategy_name VARCHAR(255) NOT NULL,
                                                                                             interval VARCHAR(20) NOT NULL,
                                                                                             data_context_size INTEGER DEFAULT 60,
                                                                                             online_mode INTEGER DEFAULT 1,
                                                                                             custom_kwargs BLOB,
                                                                                             PRIMARY KEY (name) ON CONFLICT {conflict_resolution_type});""")

        self.connection.execute(f"""CREATE TABLE IF NOT EXISTS {table_name}__live_stats (date VARCHAR(255) NOT NULL,
                                                                                         live_trader_name VARCHAR(255) NOT NULL,
                                                                                         instruments BLOB,
                                                                                         run_id VARCHAR(255) NOT NULL,
                                                                                         start_date VARCHAR(255) NOT NULL,
                                                                                         end_date VARCHAR(255) NOT NULL,
                                                                                         result BLOB);""")
        

    def get_data_providers(self):
        return self.get_timestamped_data(self.instance_name, table_name_suffixes=["data_providers"],
                                         index_col="name",
                                         skip_time_stamps=True )

    def store_data_provider(self,
                            name: str,
                            ProviderClass: str,
                            auth_cache_filepath: str,
                            StorageClass: str = "quaintscience.trader.core.persistence.sqlite.ohlc.SqliteOHLCStorage",
                            TradingBookStorageClass: str = "quaintscience.trader.core.persistence.tradebook.SqliteTradeBookStorage",
                            auth_credentials: Optional[dict] = None,
                            thread_id: str = "default",
                            run_name: Optional[str] = None,
                            custom_kwargs: Optional[dict] = None):
        key = self.init_cache_for(self.instance_name,
                                  conflict_resolution_type="REPLACE")
        if custom_kwargs is None:
            custom_kwargs = {}
        self.cache[key]["data_providers"].append({"name": name,
                                                  "ProviderClass": ProviderClass,
                                                  "auth_cache_filepath": auth_cache_filepath,
                                                  "StorageClass": StorageClass,
                                                  "TradingBookStorageClass": TradingBookStorageClass,
                                                  "auth_credentials": json.dumps(auth_credentials),
                                                  "run_name": run_name,
                                                  "thread_id": thread_id,
                                                  "custom_kwargs": json.dumps(custom_kwargs)})

    def get_brokers(self):
        return self.get_timestamped_data(self.instance_name, table_name_suffixes=["brokers"],
                                         index_col="name",
                                         skip_time_stamps=True)

    def store_broker(self,
                     name: str,
                     ProviderClass: str,
                     auth_cache_filepath: str,
                     StorageClass: str = "quaintscience.trader.core.persistence.SqliteOHLCStorage",
                     auth_credentials: Optional[dict] = None,                          
                     custom_kwargs: Optional[dict] = None):
        key = self.init_cache_for(self.instance_name,
                                  conflict_resolution_type="REPLACE")
        if custom_kwargs is None:
            custom_kwargs = {}
        self.cache[key]["data_providers"].append({"name": name,
                                                  "ProviderClass": ProviderClass,
                                                  "auth_cache_filepath": auth_cache_filepath,
                                                  "StorageClass": StorageClass,
                                                  "auth_credentials": json.dumps(auth_credentials),
                                                  "custom_kwargs": json.dumps(custom_kwargs)})

    def get_strategies(self):
         return self.get_timestamped_data(self.instance_name, table_name_suffixes=["strategies"],
                                         index_col="name",
                                         skip_time_stamps=True)


    def store_strategy(self,
                       name: str,
                       StrategyClass: str,
                       custom_kwargs: Optional[dict] = None):
        if custom_kwargs is None:
            custom_kwargs = {}
        key = self.init_cache_for(self.instance_name,
                                  conflict_resolution_type="REPLACE")
        self.cache[key]["strategies"].append({"name": name,
                                             "StrategyClass": StrategyClass,
                                             "custom_kwargs": json.dumps(custom_kwargs)})


    def get_backtesting_templates(self):
        return self.get_timestamped_data(self.instance_name, table_name_suffixes=["bt_templates"],
                                         index_col="name",
                                         skip_time_stamps=True)

    def store_backtesting_template(self,
                                   name: str,
                                   data_provider_name: str,
                                   strategy_name: str,
                                   from_date: Union[datetime.datetime, str],
                                   to_date: Union[datetime.datetime, str],
                                   interval: str = "3min",
                                   refresh_orders_immediately_on_gtt_state_change: str = False,
                                   plot_results: str = False,
                                   window_size: int = 5,
                                   live_trading_mode: bool = False,
                                   clear_tradebook_for_scrip_and_exchange: bool = False,
                                   custom_kwargs: Optional[dict] = None):
        if custom_kwargs is None:
            custom_kwargs = {}
        key = self.init_cache_for(self.instance_name,
                                  conflict_resolution_type="REPLACE")
        self.cache[key]["bt_templates"].append({"name": name,
                                                "data_provider_name": data_provider_name,
                                                "strategy_name": strategy_name,
                                                "from_date": get_datetime(from_date),
                                                "to_date": get_datetime(to_date),
                                                "interval": interval,
                                                "refresh_orders_immediately_on_gtt_state_change": refresh_orders_immediately_on_gtt_state_change,
                                                "plot_results": plot_results,
                                                "window_size": window_size,
                                                "live_trading_mode": live_trading_mode,
                                                "clear_tradebook_for_scrip_and_exchange": clear_tradebook_for_scrip_and_exchange,
                                                "custom_kwargs": json.dumps(custom_kwargs)})

    def get_backtest_stats(self):
        return self.get_timestamped_data(self.instance_name, table_name_suffixes=["bt_stats"],
                                         index_col="name",
                                         skip_time_stamps=False)

    def store_backtest_stats(self,
                             backtest_template_name: str,
                             run_id: str,
                             start_time: Union[datetime.datetime, str],
                             end_time: Union[datetime.datetime, str],
                             result: dict):
        key = self.init_cache_for(self.instance_name,
                                  conflict_resolution_type="REPLACE")
        self.cache[key]["bt_stats"].append({"backtest_template_name": backtest_template_name,
                                            "run_id": run_id,
                                            "start_time": get_datetime(start_time),
                                            "end_time": get_datetime(end_time),
                                            "date": datetime.datetime.now(),
                                            "result": json.dumps(result)})

    def get_live_templates(self):
        return self.get_timestamped_data(self.instance_name, table_name_suffixes=["live_templates"],
                                         index_col="name",
                                         skip_time_stamps=True)

    def put_live_template(self,
                          name: str,
                          data_provider_name: str,
                          broker_name: str,
                          strategy_name: str,
                          interval: str,
                          data_context_size: int,
                          online_mode: bool,
                          custom_kwargs: Optional[dict] = None):
        if custom_kwargs is None:
            custom_kwargs = {}
        key = self.init_cache_for(self.instance_name,
                                  conflict_resolution_type="REPLACE")
        self.cache[key]["live_templates"].append({"name": name,
                                                "data_provider_name": data_provider_name,
                                                "broker_name": broker_name,
                                                "strategy_name": strategy_name,
                                                "interval": interval,
                                                "data_context_size": data_context_size,
                                                "online_mode": online_mode,
                                                "custom_kwargs": json.dumps(custom_kwargs)})

    def get_live_trader_stats(self):
         return self.get_timestamped_data(self.instance_name, table_name_suffixes=["live_stats"],
                                         index_col="name",
                                         skip_time_stamps=True)

    def store_live_trader_stats(self,
                                live_trader_name: str,
                                start_time: Union[datetime.datetime, str],
                                end_time: Union[datetime.datetime, str],
                                instruments: list[str],
                                run_id: str,
                                result: dict):
        key = self.init_cache_for(self.instance_name,
                                  conflict_resolution_type="REPLACE")
        key = self.init_cache_for(self.instance_name,
                                  conflict_resolution_type="REPLACE")
        self.cache[key]["live_stats"].append({"live_trader_name": live_trader_name,
                                              "start_time": get_datetime(start_time),
                                              "end_time": get_datetime(end_time),
                                              "run_id": run_id,
                                              "date": datetime.datetime.now(),
                                              "instruments": json.dumps(instruments),
                                              "result": json.dumps(result)})
