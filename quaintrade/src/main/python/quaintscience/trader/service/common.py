from abc import abstractmethod, abstractclassmethod
from typing import Union, Optional, Type

import yaml
import configargparse

from ..core.logging import LoggerMixin
from ..core.roles import DataProvider, AuthenticatorMixin, Broker
from ..core.reflection import dynamically_load_class
from ..core.bot import Bot
from ..core.strategy import Strategy
from ..core.persistence.ohlc import OHLCStorageMixin


class Service(LoggerMixin):

    default_config_file = ".trader.env"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @abstractmethod
    def start(self):
        pass

    @classmethod
    def create_config_arg_parser(cls, default_config_file: Optional[str] = None) -> configargparse.ArgParser:
        if default_config_file is None:
            default_config_file = ".trader.env"
        p = configargparse.ArgParser(default_config_files=[default_config_file],
                                     config_file_parser_class=configargparse.YAMLConfigFileParser)
        return p


    def enrich_arg_parser(cls, p: configargparse.ArgParser) -> None:
        pass

    @classmethod
    def get_args(cls):
        parser = cls.get_arg_parser()
        return parser.parse_known_args()

    @classmethod
    def get_arg_parser(cls):
        p = DataProviderService.create_config_arg_parser(default_config_file=cls.default_config_file)
        cls.enrich_arg_parser(p)
        return p

    @classmethod
    def create_service(cls):
        p = cls.get_args()
        if isinstance(p, tuple):
            p = p[0]
        kwargs = p.__dict__
        return cls(**kwargs)

    def process_instruments_str(self, instruments: str):
        if isinstance(instruments, str):
            instruments = instruments.split(",")
            for ii, instrument in enumerate(instruments):
                parts = instrument.split(":")
                if len(parts) == 2:
                    instruments[ii] = {"scrip": parts[0], "exchange": parts[1], "type": "EQ"}
                elif len(parts) == 3:
                    instruments[ii] = {"scrip": parts[0], "exchange": parts[1], "type": parts[2]}
                else:
                    raise ValueError(f"Could not parse instrument {instrument}")
        return instruments

class DataProviderService(Service):

    def __init__(self,
                 *args,
                 data_path: str,
                 DataProviderClass: Union[str, Type[DataProvider]],
                 data_provider_login: bool = False,
                 data_provider_init: bool = False,
                 instruments: Union[str, list]=None,
                 StorageClass: Optional[Union[str, Type[OHLCStorageMixin]]] == None,
                 data_provider_auth_credentials: Optional[dict] = None,
                 data_provider_auth_cache_filepath: Optional[str] = None,
                 data_provider_reset_auth_cache: Optional[bool] = False,
                 data_provider_custom_kwargs: Optional[dict] = None,
                 **kwargs):

        Service.__init__(self, *args, **kwargs)

        if isinstance(DataProviderClass, str):
            DataProviderClass = dynamically_load_class(DataProviderClass)
        provider_kwargs = {"data_path": data_path}
        if StorageClass is not None:
            provider_kwargs["storage_class"] = StorageClass

        if issubclass(DataProviderClass, AuthenticatorMixin):
            provider_kwargs["auth_credentials"] = data_provider_auth_credentials
            provider_kwargs["auth_cache_filepath"] = data_provider_auth_cache_filepath
            provider_kwargs["reset_auth_cache"] = data_provider_reset_auth_cache
        if data_provider_custom_kwargs is None:
            data_provider_custom_kwargs = {}
        provider_kwargs.update(data_provider_custom_kwargs)
        self.data_provider = DataProviderClass(**provider_kwargs)

        if data_provider_login and isinstance(self.data_provider, AuthenticatorMixin):
            self.logger.info(f"Logging into data provider...")
            self.data_provider.login()
            
        if data_provider_init:
            self.logger.info(f"Initing data provider...")
            self.data_provider.init()

        self.instruments = self.process_instruments_str(instruments)

    @classmethod
    def enrich_arg_parser(cls, p: configargparse.ArgParser):
        p.add('--data_path', help="Data cache path", env_var="DATA_PATH")
        p.add('--data_provider_class', dest="DataProviderClass", help="Provider Class", env_var="DATA_PROVIDER_CLASS")
        p.add('--data_provider_login', help="Data provider login needed", env_var="DATA_PROVIDER_LOGIN_NEEDED", action="store_true")
        p.add('--data_provider_init', help="Do data provider init", env_var="DATA_PROVIDER_INIT_NEEDED", action="store_true")
        p.add('--data_provider_reset_auth_cache', help="Reset data provider auth cache", env_var="DATA_PROVIDER_RESET_AUTH_CACHE", action="store_true")
        p.add('--storage_class', dest="StorageClass", help="Storage class", env_var="STORAGE_CLASS")
        p.add('--data_provider_auth_credentials', help="Data provider auth Credentials", env_var="DATA_PROVIDER_AUTH_CREDENTIALS", type=yaml.safe_load)
        p.add('--data_provider_auth_cache_filepath', help="Data provider auth cache filepath", env_var="DATA_PROVIDER_AUTH_CACHE_FILEPATH")
        p.add('--data_provider_custom_kwargs', help="Data provider custom kwargs", env_var="DATA_PROVIDER_CUSTOM_KWARGS", type=yaml.safe_load)
        p.add('--instruments', help="Instruments", env_var="INSTRUMENTS")


class BrokerService(Service):

    def __init__(self,
                 BrokerClass: Union[Type[Broker], str],
                 broker_audit_records_path: str,
                 *args,
                 broker_login: bool = False,
                 broker_init: bool = False,
                 broker_skip_order_streamer: bool = False,
                 broker_auth_credentials: Optional[dict] = None,
                 broker_auth_cache_filepath: Optional[str] = None,
                 broker_reset_auth_cache: Optional[bool] = False,
                 broker_custom_kwargs: Optional[dict] = None,
                 broker_thread_id: str = "1",
                 **kwargs):
        Service.__init__(self, *args, **kwargs)
        
        if isinstance(BrokerClass, str):
            BrokerClass = dynamically_load_class(BrokerClass)
        broker_kwargs = {"audit_records_path": broker_audit_records_path,
                         "thread_id": broker_thread_id}

        if issubclass(BrokerClass, AuthenticatorMixin):
            broker_kwargs["auth_credentials"] = broker_auth_credentials
            broker_kwargs["auth_cache_filepath"] = broker_auth_cache_filepath
            broker_kwargs["reset_auth_cache"] = broker_reset_auth_cache
        if broker_custom_kwargs is None:
            broker_custom_kwargs = {}
        broker_kwargs.update(broker_custom_kwargs)
        self.broker = BrokerClass(**broker_kwargs)

        if broker_login and isinstance(self.broker, AuthenticatorMixin):
            self.logger.info("Performing broker login...")
            self.broker.login()
            
        if broker_init:
            self.logger.info("Initializing Broker...")
            self.broker.init()
        if not broker_skip_order_streamer:
            self.broker.start_order_change_streamer()
    
    @classmethod
    def enrich_arg_parser(cls, p: configargparse.ArgParser):
        p.add('--broker_class', dest="BrokerClass", help="Broker Class", env_var="BROKER_CLASS")
        p.add('--broker_login', help="Broker Login needed", env_var="BROKER_LOGIN_NEEDED", action="store_true")
        p.add('--broker_init', help="Do broker init", env_var="BROKER_INIT_NEEDED", action="store_true")
        p.add('--broker_skip_order_streamer', help="Do not start order streamer (to listen to order change)", env_var="BROKER_SKIP_ORDER_STREAMER", action="store_true")
        p.add('--broker_thread_id', help="Thread ID of broker (To store separate tradebooks)", env_var="BROKER_THREAD_ID", default=1)
        p.add('--broker_audit_records_path', help="Path to store broker audit records", env_var="BROKER_AUDIT_RECORDS_PATH")
        p.add('--broker_reset_auth_cache', help="Reset broker auth cache", env_var="BROKER_RESET_AUTH_CACHE", action="store_true")
        p.add('--broker_auth_credentials', help="Broker auth Credentials", env_var="BROKER_AUTH_CREDENTIALS", type=yaml.safe_load)
        p.add('--broker_auth_cache_filepath', help="Broker auth credentials cache filepath", env_var="BROKER_AUTH_CACHE_FILEPATH")
        p.add('--broker_custom_kwargs', help="Broker custom kwargs", env_var="BROKER_CUSTOM_KWARGS", type=yaml.safe_load)


class BotService(DataProviderService, BrokerService):

    def __init__(self,
                 StrategyClass: Union[str, Type[Strategy]],
                 *args,
                 bot_live_data_context_size: int = 60,
                 bot_backtesting_print_tables: bool = False,
                 bot_online_mode: bool = False,
                 strategy_kwargs: Optional[dict] = None,
                 bot_custom_kwargs: Optional[dict] = None,
                 **kwargs):
        
        if not hasattr(self, "data_provider"):
            DataProviderService.__init__(self, *args, **kwargs)
        if not hasattr(self, "broker"):
            BrokerService.__init__(self, *args, **kwargs)

        if isinstance(StrategyClass, str):
            StrategyClass = dynamically_load_class(StrategyClass)
        if strategy_kwargs is None:
            strategy_kwargs = {}
        if bot_custom_kwargs is None:
            bot_custom_kwargs = {}
        self.strategy = StrategyClass(**strategy_kwargs)
        bot_kwargs ={"broker": self.broker,
                     "strategy": self.strategy,
                     "data_provider": self.data_provider,
                     "live_data_context_size": bot_live_data_context_size,
                     "online_mode": bot_online_mode,
                     "backtesting_print_tables": bot_backtesting_print_tables}
        if bot_custom_kwargs is None:
            bot_custom_kwargs = {}
        bot_kwargs.update(bot_custom_kwargs)
        self.bot = Bot(**bot_kwargs)

    @classmethod
    def enrich_arg_parser(cls, p: configargparse.ArgParser):
        BrokerService.enrich_arg_parser(p)
        DataProviderService.enrich_arg_parser(p)
        p.add("--strategy_class", help="strategy to use", env_var="STRATEGY_CLASS", dest="StrategyClass")
        p.add("--bot_live_data_context_size", type=int, help="Live trading context size", env_var="BOT_LIVE_DATA_CONTEXT_SIZE")
        p.add("--bot_backtesting_print_tables", action="store_true", help="Print tables for every tick in backtesting", env_var="BOT_BACKTESTING_PRINT_TABLES")
        p.add("--bot_online_mode", action="store_true", help="Run bot in online mode (get data during live trading)", env_var="BOT_ONLINE_MODE")
        p.add('--strategy_kwargs', help="kwargs to instantiate the strategy", env_var="STRATEGY_KWARGS", type=yaml.safe_load)
        p.add('--bot_kwargs', help="kwargs to instantiate the bot", env_var="BOT_KWARGS", type=yaml.safe_load)
