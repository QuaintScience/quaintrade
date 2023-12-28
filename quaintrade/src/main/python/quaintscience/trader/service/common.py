from abc import abstractmethod
from typing import Union, Optional, Type

import configargparse

from ..core.logging import LoggerMixin
from ..integration.common import DataProvider, AuthenticatorMixin
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
                 login_needed: bool = True,
                 init: bool = True,
                 StorageClass: Optional[Union[str, Type[OHLCStorage]]] == None,
                 auth_credentials: Optional[dict] = None,
                 **kwargs):

        super().__init__(*args, **kwargs)

        if isinstance(ProviderClass, str):
            ProviderClass = dynamically_load_class(ProviderClass)

        provider_kwargs = {"data_path": data_path}
        if StorageClass is not None:
            provider_kwargs["storage_class"] = StorageClass
        
        if issubclass(ProviderClass, AuthenticatorMixin):
            provider_kwargs["auth_credentials"] = auth_credentials
        
        self.data_provider = ProviderClass(**provider_kwargs)

        if login_needed and isinstance(self.data_provider, AuthenticatorMixin):
            self.data_provider.login()
            
        if init:
            self.data_provider.init()

    @classmethod
    def get_arg_parser(cls) -> configargparse.ArgParser:

        p = configargparse.ArgParser(default_config_files=['.trader.env'])
        p.add('--data_path', help="Data cache path", env_var="DATA_PATH")
        p.add('--provider_class', help="Provider", env_var="PROVIDER_CLASS")
        p.add('--from_date', help="From date", env_var="FROM_DATE")
        p.add('--to_date', help="To date", env_var="TO_DATE")
        p.add('--instruments', help="Instruments", env_var="INSTRUMENTS")
        p.add('--login_needed', help="Login needed", env_var="LOGIN_NEEDED")
        p.add('--init', help="Do init", env_var="INIT")
        p.add('--storage_class', help="Storage class", env_var="STORAGE_CLASS")
        p.add('--auth_credentials_file', help="API key", env_var="AUTH_CREDENTIALS")
        return p
