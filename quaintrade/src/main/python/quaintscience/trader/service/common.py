from abc import abstractmethod
from typing import Union, Optional, Type

import yaml
import configargparse

from ..core.logging import LoggerMixin
from ..core.roles import DataProvider, AuthenticatorMixin
from ..core.reflection import dynamically_load_class
from ..core.persistence import OHLCStorage


class Service(LoggerMixin):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @abstractmethod
    def start(self):
        pass

    @classmethod
    def get_arg_parser(cls) -> configargparse.ArgParser:
        raise NotImplementedError("get_args not implemented; needs to return options")

    @classmethod
    def get_args(cls):
        parser = cls.get_arg_parser()
        return parser.parse_known_args()

    @classmethod
    def create_service(cls):
        p = cls.get_args()
        if isinstance(p, tuple):
            p = p[0]
        kwargs = p.__dict__
        return cls(**kwargs)


class DataProviderService(Service):

    def __init__(self,
                 *args,
                 data_path: str,
                 ProviderClass: Union[str, Type[DataProvider]],
                 login: bool = False,
                 init: bool = False,
                 instruments: Union[str, list]=None,
                 StorageClass: Optional[Union[str, Type[OHLCStorage]]] == None,
                 auth_credentials: Optional[dict] = None,
                 auth_cache_filepath: Optional[str] = None,
                 reset_auth_cache: Optional[bool] = False,
                 **kwargs):

        super().__init__(*args, **kwargs)

        if isinstance(ProviderClass, str):
            ProviderClass = dynamically_load_class(ProviderClass)
        provider_kwargs = {"data_path": data_path}
        if StorageClass is not None:
            provider_kwargs["storage_class"] = StorageClass

        if issubclass(ProviderClass, AuthenticatorMixin):
            provider_kwargs["auth_credentials"] = auth_credentials
            provider_kwargs["auth_cache_filepath"] = auth_cache_filepath
            provider_kwargs["reset_auth_cache"] = reset_auth_cache
        self.data_provider = ProviderClass(**provider_kwargs)

        if login and isinstance(self.data_provider, AuthenticatorMixin):
            self.data_provider.login()
            
        if init:
            self.data_provider.init()

        if isinstance(instruments, str):
            instruments = [{"scrip": value.split(":")[0], "exchange": value.split(":")[1]}
                           for value in instruments.split(",")]
        self.instruments = instruments

    @classmethod
    def get_arg_parser(cls) -> configargparse.ArgParser:

        p = configargparse.ArgParser(default_config_files=['.trader.env'],
                                     config_file_parser_class=configargparse.YAMLConfigFileParser)
        p.add('--data_path', help="Data cache path", env_var="DATA_PATH")
        p.add('--provider_class', dest="ProviderClass", help="Provider Class", env_var="PROVIDER_CLASS")
        p.add('--login_needed', help="Login needed", env_var="LOGIN_NEEDED", action="store_true")
        p.add('--init', help="Do init", env_var="INIT", action="store_true")
        p.add('--reset_auth_cache', help="Reset auth cache", env_var="RESET_AUTH_CACHE", action="store_true")
        p.add('--storage_class', dest="StorageClass", help="Storage class", env_var="STORAGE_CLASS")
        p.add('--auth_credentials', help="Auth Credentials", env_var="AUTH_CREDENTIALS", type=yaml.safe_load)
        p.add('--auth_cache_filepath', help="Auth Credentials", env_var="AUTH_CACHE_FILEPATH")
        p.add('--instruments', help="Instruments", env_var="INSTRUMENTS")
        return p
